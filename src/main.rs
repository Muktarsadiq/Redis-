/* Imports */
use std::io::{self, Read, Write};
use std::ops::Deref;
use std::net::SocketAddr;
use socket2::{Socket, Domain, Type, Protocol, SockAddr};
use std::env;
use errno::{errno, set_errno, Errno};
use nix::poll::{poll, PollFd, PollFlags};
use std::collections::HashMap;
use std::os::unix::io::{AsFd, AsRawFd, RawFd};
use intrusive_collections::{LinkedList, LinkedListLink, intrusive_adapter, linked_list::CursorMut,};

use std::sync::{Arc, Mutex, OnceLock, Condvar};
use std::cell::RefCell;
use std::cmp::{Ordering, max};
use std::rc::Rc;
use ordered_float::OrderedFloat;

use std::thread;
use std::collections::VecDeque;

use std::time::{Instant, SystemTime, UNIX_EPOCH};


/* Constants */
const BACKLOG: i32 = 128;
const K_MAX_MSG: usize = 4096;
/// Maximum load factor for chaining hash tables.
/// A value > 1 is valid because multiple items can occupy one bucket.
#[allow(dead_code)]
const K_MAX_LOAD_FACTOR: usize = 8;
#[allow(dead_code)]
const K_REHASHING_WORK: usize = 128;

const K_IDLE_TIMEOUT_MS: u64 = 5000; // 5 seconds
const K_MAX_WORKS: usize = 2000;
const K_LARGE_CONTAINER_SIZE: usize = 1000;
static GLOBAL_DATA: OnceLock<Mutex<GData>> = OnceLock::new();




/*Struct */
#[derive(Debug)]
struct Buffer {
    data: Vec<u8>,
    start: usize,  // Beginning of valid data
    end: usize,    // End of valid data (exclusive)
}

impl Buffer {
    fn new() -> Self {
        Self {
            data: Vec::with_capacity(4096), // Start with reasonable capacity
            start: 0,
            end: 0,
        }
    }
    
    fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
            start: 0,
            end: 0,
        }
    }
    
    // Get the current data as a slice
    fn data(&self) -> &[u8] {
        &self.data[self.start..self.end]
    }
    
    // Get current data length
    fn len(&self) -> usize {
        self.end - self.start
    }
    
    fn is_empty(&self) -> bool {
        self.start == self.end
    }
    
    // Append data to the buffer
    fn append(&mut self, data: &[u8]) {
        let needed_space = data.len();
        
        // Check if we need to make room
        if self.end + needed_space > self.data.capacity() {
            self.make_room(needed_space);
        }
        
        // Ensure we have enough capacity
        if self.end + needed_space > self.data.len() {
            self.data.resize(self.end + needed_space, 0);
        }
        
        // Copy the data
        self.data[self.end..self.end + needed_space].copy_from_slice(data);
        self.end += needed_space;
    }
    
    // Remove n bytes from the front (O(1) operation!)
    fn consume(&mut self, n: usize) {
        assert!(n <= self.len(), "Cannot consume more bytes than available");
        
        self.start += n;
        
        // If buffer is now empty, reset pointers to beginning
        if self.start == self.end {
            self.start = 0;
            self.end = 0;
        }
    }
    
    // Get a slice of the first n bytes without consuming them
    fn peek(&self, n: usize) -> Option<&[u8]> {
        if n <= self.len() {
            Some(&self.data[self.start..self.start + n])
        } else {

            None
        }
    }
    
    // Make room for new data by either moving existing data to front
    // or reallocating if necessary
    fn make_room(&mut self, needed: usize) {
        let current_len = self.len();
        let available_at_end = self.data.capacity() - self.end;
        let available_at_start = self.start;
        
        // If we can fit by moving data to the front, do that
        if available_at_start + available_at_end >= needed {
            // Move existing data to the beginning
            if current_len > 0 {
                self.data.copy_within(self.start..self.end, 0);
            }
            self.start = 0;
            self.end = current_len;
        } else {
            // Need to reallocate - grow capacity
            let new_capacity = (self.data.capacity() * 2).max(current_len + needed);
            let mut new_data = Vec::with_capacity(new_capacity);
            
            // Copy existing data to new buffer
            if current_len > 0 {
                new_data.extend_from_slice(&self.data[self.start..self.end]);
            }
            
            self.data = new_data;
            self.start = 0;
            self.end = current_len;
        }
    }

    fn response_begin(&mut self) -> usize {
        let header_pos = self.len();
        self.append_u32(0); // Reserve 4 bytes with placeholder
        header_pos
    }
    
    // Calculate current response size (excluding header)
    fn response_size(&self, header_pos: usize) -> usize {
        self.len() - header_pos - 4
    }
    
    // Finalize response - write actual length to reserved header
    fn response_end(&mut self, header_pos: usize) {
        let mut msg_size = self.response_size(header_pos);
        
        // Check if response is too big
        if msg_size > K_MAX_MSG {
            // Truncate buffer and write error instead
            self.end = header_pos + 4; // Reset to just after header
            out_err(self, "response is too big");
            msg_size = self.response_size(header_pos);
        }
        
        // Write actual length to the reserved header position
        let len_bytes = (msg_size as u32).to_le_bytes();
        self.data[self.start + header_pos..self.start + header_pos + 4]
            .copy_from_slice(&len_bytes);
    }


    // Add the new methods here:
    fn append_u8(&mut self, data: u8) {
        self.append(&[data]);
    }
    
    fn append_u32(&mut self, data: u32) {
        self.append(&data.to_le_bytes());
    }
    
    fn append_i64(&mut self, data: i64) {
        self.append(&data.to_le_bytes());
    }
    
    fn append_f64(&mut self, data: f64) {
        self.append(&data.to_le_bytes());
    }

    fn out_begin_arr(&mut self) -> usize {
        self.append_u8(Tag::Arr as u8);
        let ctx = self.len() - 4; // Position where count will go
        self.append_u32(0); // Placeholder count
        ctx
    }
    
    fn out_end_arr(&mut self, ctx: usize, count: u32) {
        // Verify we're patching an array tag
        assert_eq!(self.data[self.start + ctx], Tag::Arr as u8);
        
        // Patch the count at the saved position
        let count_bytes = count.to_le_bytes();
        self.data[self.start + ctx + 1..self.start + ctx + 5]
            .copy_from_slice(&count_bytes);
    }
}

// Extension trait to make it work like Vec for easy migration
impl Buffer {
    // For compatibility with your existing code patterns
    fn extend_from_slice(&mut self, data: &[u8]) {
        self.append(data);
    }
    
    // Drain n bytes from the front (for compatibility)
    fn drain_front(&mut self, n: usize) {
        self.consume(n);
    }
}

// Implement Deref so Buffer can be used like a slice automatically
impl Deref for Buffer {
    type Target = [u8];
    
    fn deref(&self) -> &Self::Target {
        self.data()
    }
}

type Work = Box<dyn FnOnce() + Send + 'static>;

pub struct ThreadPool {
    threads: Vec<thread::JoinHandle<()>>,
     queue: Arc<(Mutex<VecDeque<Work>>, Condvar)>,
    shutdown: Arc<Mutex<bool>>,
}

impl ThreadPool {
    pub fn new(num_threads: usize) -> Self {
        let queue: Arc<(Mutex<VecDeque<Work>>, Condvar)> = Arc::new((
            Mutex::new(VecDeque::new()), // Now it knows it's VecDeque<Work>
            Condvar::new()
        ));
        let shutdown = Arc::new(Mutex::new(false));
        let mut threads = Vec::with_capacity(num_threads);
        
        for _ in 0..num_threads {
            let queue_clone = queue.clone();
            let shutdown_clone = shutdown.clone();
            
            let handle = thread::spawn(move || {
                loop {
                    let work = {
                        let (lock, cvar) = &*queue_clone;
                        let mut q = lock.lock().unwrap();
                        
                        // Wait for work if empty
                        while q.is_empty() {
                            // Check for shutdown signal
                            if *shutdown_clone.lock().unwrap() {
                                return; // Exit thread
                            }
                            q = cvar.wait(q).unwrap();
                        }
                        
                        // Check shutdown again after wakeup
                        if *shutdown_clone.lock().unwrap() {
                            return;
                        }
                        
                        q.pop_front() // Remove from front like deque
                    };
                    
                    if let Some(job) = work {
                        job(); 
                    }
                }
            });
            threads.push(handle);
        }
        
        Self { threads, queue, shutdown }
    }
    
    pub fn submit<F>(&self, job: F) 
    where 
        F: FnOnce() + Send + 'static,
    {
        let (lock, cvar) = &*self.queue;
        let mut q = lock.lock().unwrap();
        q.push_back(Box::new(job));
        cvar.notify_one();
    }
    
    // Graceful shutdown
    pub fn shutdown(self) {
        // Signal all threads to stop
        *self.shutdown.lock().unwrap() = true;
        
        // Wake up all sleeping threads
        let (_, cvar) = &*self.queue;
        cvar.notify_all();
        
        // Wait for all threads to finish
        for handle in self.threads {
            handle.join().unwrap();
        }
    }
}


#[derive(Debug)]
pub struct Node<T> {
    key: T,
    left: Option<Rc<RefCell<Node<T>>>>,
    right: Option<Rc<RefCell<Node<T>>>>,
}

impl<T> Node<T> {
    fn new(key: T) -> Rc<RefCell<Self>> {
        Rc::new(RefCell::new(Node {
            key,
            left: None,
            right: None,
        }))
    }
}

#[derive(Debug)]
struct AvlNode<T> {
    parent: Option<Rc<RefCell<AvlNode<T>>>>,
    left: Option<Rc<RefCell<AvlNode<T>>>>,
    right: Option<Rc<RefCell<AvlNode<T>>>>,
    height: u32,
    key: T,
}

impl<T> AvlNode<T> {
    fn new(key: T) -> Rc<RefCell<Self>> {
        Rc::new(RefCell::new(Self {
            parent: None,
            left: None,
            right: None,
            height: 1, 
            key,
        }))
    }
}

fn tree_search<T: Ord>(
    from: &Option<Rc<RefCell<Node<T>>>>,
    key: &T,
) -> Option<Rc<RefCell<Node<T>>>> {
    match from {
        None => None,
        Some(node_rc) => {
            let node_ref = node_rc.borrow();
            match key.cmp(&node_ref.key) {
                Ordering::Less => tree_search::<T>(&node_ref.left, key),
                Ordering::Greater => tree_search::<T>(&node_ref.right, key),
                Ordering::Equal => Some(node_rc.clone()),
            }
        }
    }
}

fn tree_insert<T: Ord>(root: &mut Option<Rc<RefCell<Node<T>>>>, key: T) {
    match root {
        None => {
            *root = Some(Node::new(key));
        }
        Some(node_rc) => {
            let mut node_ref = node_rc.borrow_mut();
            if key < node_ref.key {
                tree_insert::<T>(&mut node_ref.left, key);
            } else if key > node_ref.key {
                tree_insert::<T>(&mut node_ref.right, key);
            }
            // if equal, do nothing (no dups)
        }
    }
}

fn tree_delete<T: Ord + Clone>(
    node: Option<Rc<RefCell<Node<T>>>>,
    key: &T,
) -> Option<Rc<RefCell<Node<T>>>> {
    match node {
        None => None, // not found
        Some(node_rc) => {
            let ordering = {
                // borrow just for comparison
                let node_ref = node_rc.borrow();
                key.cmp(&node_ref.key)
            };

            match ordering {
                std::cmp::Ordering::Less => {
                    // go left
                    let left_child = node_rc.borrow().left.clone();
                    let new_left = tree_delete(left_child, key);
                    node_rc.borrow_mut().left = new_left;
                    Some(node_rc.clone())
                }
                std::cmp::Ordering::Greater => {
                    // go right
                    let right_child = node_rc.borrow().right.clone();
                    let new_right = tree_delete(right_child, key);
                    node_rc.borrow_mut().right = new_right;
                    Some(node_rc.clone())
                }
                std::cmp::Ordering::Equal => {
                    // found node -> detach
                    node_detach(node_rc)
                }
            }
        }
    }
}



// Remove a node and fix tree structure
fn node_detach<T: Ord + Clone >(node_rc: Rc<RefCell<Node<T>>>) -> Option<Rc<RefCell<Node<T>>>> {
    let mut node_ref = node_rc.borrow_mut();

    // Case 1: no right child → return left
    if node_ref.right.is_none() {
        return node_ref.left.take();
    }

    // Case 2: no left child → return right
    if node_ref.left.is_none() {
        return node_ref.right.take();
    }

    // Case 3: both children → find successor
    let mut succ_parent = node_rc.clone();
    let mut succ_rc = node_ref.right.clone().unwrap();

    // Go left as far as possible
    loop {
        let left_opt = succ_rc.borrow().left.clone();
        if let Some(left_rc) = left_opt {
            succ_parent = succ_rc.clone();
            succ_rc = left_rc;
        } else {
            break;
        }
    }

    // Take successor's key
    let succ_key = succ_rc.borrow().key.clone();

    // Recursively delete the successor (easy case, since no left child)
    if Rc::ptr_eq(&succ_parent, &node_rc) {
        // Successor is right child
        succ_parent.borrow_mut().right = node_detach(succ_rc.clone());
    } else {
        succ_parent.borrow_mut().left = succ_rc.borrow().right.clone();
    }

    // Replace current node's key with successor's key
    node_ref.key = succ_key;

    Some(node_rc.clone())
}

fn avl_height<T>(node: &Option<Rc<RefCell<AvlNode<T>>>>) -> u32 {
    match node {
        Some(node_rc) => node_rc.borrow().height,
        None => 0,
    }
}

fn avl_update<T>(node: &Rc<RefCell<AvlNode<T>>>) {
    let left_height = {
        let borrowed = node.borrow();
        avl_height(&borrowed.left)
    };
    let right_height = {
        let borrowed = node.borrow();
        avl_height(&borrowed.right)
    };

    node.borrow_mut().height = 1 + max(left_height, right_height);
}

fn rot_left<T>(node: Rc<RefCell<AvlNode<T>>>) -> Rc<RefCell<AvlNode<T>>> {
    // Step 1: unwrap relationships
    let parent_opt = node.borrow().parent.clone();
    let new_node = node
        .borrow()
        .right
        .clone()
        .expect("Right child must exist for left rotation");
    let inner = new_node.borrow().left.clone();

    {
        // Step 2: relink node -> inner
        let mut node_mut = node.borrow_mut();
        node_mut.right = inner.clone();
    }
    if let Some(inner_node) = &inner {
        inner_node.borrow_mut().parent = Some(node.clone());
    }

    {
        // Step 3: new_node -> parent
        new_node.borrow_mut().parent = parent_opt.clone();
    }

    {
        // Step 4: new_node <-> node
        new_node.borrow_mut().left = Some(node.clone());
        node.borrow_mut().parent = Some(new_node.clone());
    }

    // Step 5: update heights bottom-up
    avl_update(&node);
    avl_update(&new_node);

    new_node
}

fn rot_right<T>(node: Rc<RefCell<AvlNode<T>>>) -> Rc<RefCell<AvlNode<T>>> {
    // Grab the pivot (left child)
    let pivot = {
        let node_borrow = node.borrow();
        node_borrow.left.clone().expect("Left child must exist for right rotation")
    };

    let pivot_right = pivot.borrow().right.clone();

    // Step 1: node.left <- pivot.right
    {
        let mut node_mut = node.borrow_mut();
        node_mut.left = pivot_right.clone();
    }
    if let Some(pivot_right_node) = &pivot_right {
        pivot_right_node.borrow_mut().parent = Some(node.clone());
    }

    // Step 2: pivot.parent <- node.parent
    let parent = node.borrow().parent.clone();
    {
        pivot.borrow_mut().parent = parent.clone();
    }

    // Step 3: reconnect parent’s link to pivot
    if let Some(p) = parent {
        let mut p_mut = p.borrow_mut();
        if p_mut.left.as_ref().map_or(false, |left| Rc::ptr_eq(left, &node)) {
            p_mut.left = Some(pivot.clone());
        } else if p_mut.right.as_ref().map_or(false, |right| Rc::ptr_eq(right, &node)) {
            p_mut.right = Some(pivot.clone());
        }

    }

    // Step 4: pivot.right <- node
    {
        let mut pivot_mut = pivot.borrow_mut();
        pivot_mut.right = Some(node.clone());
    }
    node.borrow_mut().parent = Some(pivot.clone());

    // Step 5: update heights bottom-up
    avl_update(&node);
    avl_update(&pivot);

    pivot
}



fn avl_fix_left<T>(node: Rc<RefCell<AvlNode<T>>>) -> Rc<RefCell<AvlNode<T>>> {
    // Step 1: extract left child (drop after use)
    let (left_left_height, left_right_height, needs_double) = {
        let node_borrow = node.borrow();
        let left_child = node_borrow.left.clone().expect("Left child must exist");

        let left_left_height = avl_height(&left_child.borrow().left);
        let left_right_height = avl_height(&left_child.borrow().right);

        (left_left_height, left_right_height, left_left_height < left_right_height)
    };

    // Step 2: fix left-right case if needed
    if needs_double {
        let left_child = node.borrow().left.clone().unwrap();
        let new_left = rot_left(left_child);

        // reconnect new subtree root as node.left
        {
            let mut node_mut = node.borrow_mut();
            node_mut.left = Some(new_left.clone());
        }
        new_left.borrow_mut().parent = Some(node.clone());
    }

    // Step 3: always rotate right on node
    rot_right(node)
}

fn avl_fix_right<T>(node: Rc<RefCell<AvlNode<T>>>) -> Rc<RefCell<AvlNode<T>>> {
    // Clone right child (must exist in imbalance case)
    let right_child = {
        node.borrow().right.clone().expect("Right child must exist")
    };

    // Check if this is a right-left case (double rotation needed)
    let (rl_height, rr_height) = {
        let rc_ref = right_child.borrow();
        (avl_height(&rc_ref.left), avl_height(&rc_ref.right))
    };

    if rr_height < rl_height {
        // Right-left case: rotate right first on right child
        let new_right = rot_right(right_child);
        node.borrow_mut().right = Some(new_right);
    }

    // Always finish with a left rotation
    rot_left(node)
}


fn avl_fix<T>(mut node: Rc<RefCell<AvlNode<T>>>) -> Rc<RefCell<AvlNode<T>>> {
    loop {
        let parent_opt = node.borrow().parent.clone();

        // Update auxiliary data
        avl_update(&node);

        // Check for imbalance
        let (left_height, right_height) = {
            let nb = node.borrow();
            (avl_height(&nb.left), avl_height(&nb.right))
        };

        // Fix imbalance if needed
        let mut fixed = node.clone();
        if left_height == right_height + 2 {
            fixed = avl_fix_left(node);
        } else if left_height + 2 == right_height {
            fixed = avl_fix_right(node);
        }

        // If parent exists, reattach and move up
        if let Some(parent) = parent_opt {
            let is_left_child = {
                let parent_ref = parent.borrow();
                parent_ref.left.as_ref().map(|l| Rc::ptr_eq(l, &fixed)).unwrap_or(false)
            };

            if is_left_child {
                parent.borrow_mut().left = Some(fixed.clone());
            } else {
                parent.borrow_mut().right = Some(fixed.clone());
            }

            node = parent; // continue upward
        } else {
            // No parent => root found
            return fixed;
        }
    }
}


fn avl_del_easy<T>(node: Rc<RefCell<AvlNode<T>>>) -> Option<Rc<RefCell<AvlNode<T>>>> {
    // Precondition: at most 1 child
    {
        let node_ref = node.borrow();
        assert!(
            !(node_ref.left.is_some() && node_ref.right.is_some()),
            "Node must have at most 1 child"
        );
    }

    // Extract child and parent
    let child = {
        let node_ref = node.borrow();
        node_ref.left.clone().or_else(|| node_ref.right.clone())
    };
    let parent = node.borrow().parent.clone();

    // Update child's parent pointer
    if let Some(ref child_ref) = child {
        child_ref.borrow_mut().parent = parent.clone();
    }

    // If node is root, return new root (child or None)
    if parent.is_none() {
        return child;
    }

    // Attach child to the correct side of parent
    let parent_node = parent.unwrap();
    let is_left_child = {
        let parent_ref = parent_node.borrow();
        parent_ref.left.as_ref().map(|left| Rc::ptr_eq(left, &node)).unwrap_or(false)
    };

    if is_left_child {
        parent_node.borrow_mut().left = child;
    } else {
        parent_node.borrow_mut().right = child;
    }

    // Rebalance from parent upward
    Some(avl_fix(parent_node))
}

fn avl_del<T: Clone>(node: Rc<RefCell<AvlNode<T>>>) -> Option<Rc<RefCell<AvlNode<T>>>> {
    // Easy case: 0 or 1 child
    let (has_left, has_right) = {
        let node_ref = node.borrow();
        (node_ref.left.is_some(), node_ref.right.is_some())
    };
    
    if !has_left || !has_right {
        return avl_del_easy(node);
    }

    // Hard case: find successor (leftmost node in right subtree)
    let mut victim = {
        let node_ref = node.borrow();
        node_ref.right.clone().expect("Right child must exist")
    };

    loop {
        let next = {
            let v_ref = victim.borrow();
            v_ref.left.clone()
        };
        match next {
            Some(left_child) => victim = left_child,
            None => break,
        }
    }

    // Swap keys instead of copying whole structs
    let victim_key = victim.borrow().key.clone();
    let node_key = node.borrow().key.clone();

    victim.borrow_mut().key = node_key;
    node.borrow_mut().key = victim_key;

    // Now delete the victim (which has the original key)
    avl_del_easy(victim)
}

fn avl_insert<T: Ord>(
    root: &mut Option<Rc<RefCell<AvlNode<T>>>>, 
    new_node: Rc<RefCell<AvlNode<T>>>
) {
    let mut current = root.clone();
    let mut parent: Option<Rc<RefCell<AvlNode<T>>>> = None;
    let mut is_left_child = false;
    
    while let Some(node) = current {
        parent = Some(node.clone());
        let cmp = new_node.borrow().key.cmp(&node.borrow().key);
        
        if cmp == std::cmp::Ordering::Less {
            current = node.borrow().left.clone();
            is_left_child = true;
        } else {
            current = node.borrow().right.clone();
            is_left_child = false;
        }
    }
    
    new_node.borrow_mut().parent = parent.clone();
    
    if let Some(parent_node) = parent {
        if is_left_child {
            parent_node.borrow_mut().left = Some(new_node.clone());
        } else {
            parent_node.borrow_mut().right = Some(new_node.clone());
        }
        // Fix from the newly inserted node
        *root = Some(avl_fix(new_node.clone()));
    } else {
        *root = Some(new_node);
    }
}

fn avl_search_and_delete<T: Ord + Clone>(
    root: &mut Option<Rc<RefCell<AvlNode<T>>>>,
    key: &T,
) -> Option<Rc<RefCell<AvlNode<T>>>> {
    let mut current = root.clone();

    while let Some(node) = current {
        // Borrow only long enough to compare
        let ordering = {
            let node_ref = node.borrow();
            key.cmp(&node_ref.key)
        };

        match ordering {
            std::cmp::Ordering::Less => {
                current = node.borrow().left.clone();
            }
            std::cmp::Ordering::Greater => {
                current = node.borrow().right.clone();
            }
            std::cmp::Ordering::Equal => {
                // Found the node -> delegate actual removal
                *root = avl_del(node.clone());
                return Some(node);
            }
        }
    }

    None // Key not found
}

fn avl_offset(
    mut node: Option<Arc<Mutex<ZNode>>>, 
    offset: i64
) -> Option<Arc<Mutex<ZNode>>> {
    let mut position = 0i64;
    
    while offset != position {
        let node_rc = match &node {
            Some(n) => n.clone(),
            None => return None,
        };
        
        // Get counts for decision making
        let (right_count, left_count) = {
            let borrowed = node_rc.lock().unwrap() ;
            (
                avl_count(borrowed.tree_right.clone()),
                avl_count(borrowed.tree_left.clone())
            )
        };
        
        if position < offset && position + right_count as i64 >= offset {
            // Target is inside the right subtree
            let right_child = {
                let borrowed = node_rc.lock().unwrap();
                borrowed.tree_right.clone()
            };
            node = right_child;
            
            if let Some(new_node) = &node {
                let left_count_new = avl_count(new_node.lock().unwrap().tree_left.clone());
                position += left_count_new as i64 + 1;
            }
            
        } else if position > offset && position - left_count as i64 <= offset {
            // Target is inside the left subtree  
            let left_child = {
                let borrowed = node_rc.lock().unwrap();
                borrowed.tree_left.clone()
            };
            node = left_child;
            
            if let Some(new_node) = &node {
                let right_count_new = avl_count(new_node.lock().unwrap().tree_right.clone());
                position -= right_count_new as i64 + 1;
            }
            
        } else {
            // Go to the parent
            let parent = {
                let borrowed = node_rc.lock().unwrap() ;
                borrowed.tree_parent.clone()
            };
            
            match parent {
                Some(parent_rc) => {
                    // Check if current node is right child of parent
                    let is_right_child = {
                        let parent_borrowed = parent_rc.lock().unwrap();
                        parent_borrowed.tree_right.as_ref()
                            .map_or(false, |right| Arc::ptr_eq(right, &node_rc))
                    };
                    
                    if is_right_child {
                        position -= left_count as i64 + 1;
                    } else {
                        position += right_count as i64 + 1;
                    }
                    
                    node = Some(parent_rc);
                }
                None => return None,
            }
        }
    }
    
    node
}



fn successor(node: Option<Arc<Mutex<ZNode>>>) -> Option<Arc<Mutex<ZNode>>> {
    let node_rc = node?;
    
    // Case 1: find the leftmost node in the right subtree
    if let Some(right_child) = node_rc.lock().unwrap() .tree_right.clone() {
        let mut current = right_child;
        loop {
            let left_child = current.lock().unwrap().tree_left.clone(); // Separate borrow
            match left_child {
                Some(child) => current = child,  // Now we can assign
                None => break,
            }
        }
        return Some(current);
    }
    
    // Case 2: find the ancestor where I'm the rightmost node in the left subtree  
    let mut current = node_rc;
    loop {
        let parent = current.lock().unwrap().tree_parent.clone(); // Separate borrow
        match parent {
            Some(parent_rc) => {
                let is_left_child = parent_rc.lock().unwrap().tree_left.as_ref()
                    .map_or(false, |left| Arc::ptr_eq(left, &current));
                
                if is_left_child {
                    return Some(parent_rc);
                }
                current = parent_rc;  // Now we can assign
            }
            None => break,
        }
    }
    
    None
}



fn avl_count(node: Option<Arc<Mutex<ZNode>>>) -> u32 {
    match node {
        Some(rc_node) => rc_node.lock().unwrap() .tree_count,
        None => 0,
    }
}

/* Timer and Timeout */
fn get_monotonic_time_ms() -> u64 {
	//use a static start time to measure elapsed time
	static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
	let start = START.get_or_init(|| Instant::now());
		
	start.elapsed().as_millis() as u64
}

/// Monotonic clock in nanoseconds (closer to timespec precision)
fn get_monotonic_time_ns() -> u128 {
    static START: OnceLock<Instant> = OnceLock::new();
    let start = START.get_or_init(|| Instant::now());
    start.elapsed().as_nanos()
}

/// Wall clock (like CLOCK_REALTIME), in milliseconds since Unix epoch
fn get_current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}


type Link = Option<Arc<Mutex<DList>>>;

#[derive(Debug)]
struct DList {
    prev: Link,
    next: Link,
}

impl DList {
     fn new() -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(DList {
            prev: None,
            next: None,
        }))
    }
}

fn dlist_detach(node: Arc<Mutex<DList>>) {
    let (prev, next) = {
        let node_ref = node.lock().unwrap();
        (node_ref.prev.clone(), node_ref.next.clone())
    };
    
    // Update prev node's next pointer
    if let Some(prev_node) = &prev {
        prev_node.lock().unwrap().next = next.clone();
    }
    
    // Update next node's prev pointer  
    if let Some(next_node) = &next {
        next_node.lock().unwrap().prev = prev; // Use prev directly, not prev.clone()
    }
    
    // Clear the detached node's pointers
    let mut node_mut = node.lock().unwrap();
    node_mut.prev = None;
    node_mut.next = None;
}

fn dlist_init(node: Arc<Mutex<DList>>) {
    let mut node_mut = node.lock().unwrap();
    node_mut.prev = Some(node.clone());  // This should now work correctly
    node_mut.next = Some(node.clone());
}


fn dlist_empty(node: Arc<Mutex<DList>>) -> bool {
    let node_ref = node.lock().unwrap();
    match &node_ref.next {
        Some(next) => Arc::ptr_eq(&node, next),
        None => true,
    }
}

fn dlist_insert_before(target: &Arc<Mutex<DList>>, rookie: &Arc<Mutex<DList>>) {
    // Get the previous node
    let prev = {
        let t = target.lock().unwrap();
        t.prev.clone().expect("Target should have prev pointer")
    };

    // Update all connections carefully to avoid deadlocks
    {
        let mut p = prev.lock().unwrap();
        p.next = Some(rookie.clone());
    }

    {
        let mut r = rookie.lock().unwrap();
        r.prev = Some(prev);
        r.next = Some(target.clone());
    }

    {
        let mut t = target.lock().unwrap();
        t.prev = Some(rookie.clone());
    }
}

fn next_timer_ms() -> i32 {
    let now_ms = get_monotonic_time_ms();
    let mut next_ms = u64::MAX;

    with_global_data(|g_data| {
        // Idle timers using linked list
        if !dlist_empty(g_data.idle_list.clone()) {
            let first_node = {
                let idle_list_ref = g_data.idle_list.lock().unwrap(); // Changed from borrow()
                idle_list_ref.next.clone()
            };
            
            if let Some(first_node_rc) = first_node {
                // Find which connection owns this idle node
                for (_, conn) in &g_data.fd2conn {
                    if Arc::ptr_eq(&conn.idle_node, &first_node_rc) { // Changed from Rc::ptr_eq
                        next_ms = conn.last_active_ms + K_IDLE_TIMEOUT_MS;
                        break;
                    }
                }
            }
        }

        // TTL timers using heap
        if !g_data.heap.is_empty() && g_data.heap[0].value < next_ms {
            next_ms = g_data.heap[0].value;
        }
    });

    // Return timeout value
    if next_ms == u64::MAX {
        -1 // No timers
    } else if next_ms <= now_ms {
        0 // Expired/missed
    } else {
        (next_ms - now_ms) as i32
    }
}


fn process_timers() {
    let now_ms = get_monotonic_time_ms();

    with_global_data(|g_data| {
        // Idle timers (linked list)
        let mut expired_fds = Vec::new();
        
        loop {
            let first_node = {
                let idle_list_ref = g_data.idle_list.lock().unwrap(); // Changed from borrow()
                idle_list_ref.next.clone()
            };

            let Some(first_node_rc) = first_node else {
                break; // list empty
            };

            // Find which connection owns this idle node
            let mut found_expired = false;
            for (&fd, conn) in &g_data.fd2conn {
                if Arc::ptr_eq(&conn.idle_node, &first_node_rc) { // Changed from Rc::ptr_eq
                    let expire_at = conn.last_active_ms + K_IDLE_TIMEOUT_MS;
                    if expire_at < now_ms {
                        println!("Idle connection expired: {}", fd);
                        expired_fds.push(fd);
                        found_expired = true;
                    }
                    break;
                }
            }
            
            if !found_expired {
                break; // First connection not expired, so none are
            }
        }
        
        // Remove expired connections
        for fd in expired_fds {
            if let Some(conn) = g_data.fd2conn.remove(&fd) {
                dlist_detach(conn.idle_node.clone());
            }
        }

        // TTL timers (heap)
        let mut nworks = 0;
        while !g_data.heap.is_empty()
            && g_data.heap[0].value < now_ms
            && nworks < K_MAX_WORKS
        {
            let heap_item = &g_data.heap[0];
            let entry_ref = heap_item.entry_ref.clone();

            // Delete from DB
            {
                let entry = entry_ref.lock().unwrap(); // Changed from borrow()
                g_data.db.delete_entry(&entry.key);
                println!("TTL expired for key: {}", entry.key);
            }

            // Delete from heap
            heap_delete(&mut g_data.heap, 0);

            nworks += 1;
        }
    });
}


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ValueType {
    Init = 0,
    Str = 1,
    ZSet = 2,
}

#[derive(Debug)]
pub enum Value {
    Init,
    Str(String),
    ZSet(ZSet),
}

impl Value {
    pub fn value_type(&self) -> ValueType {
        match self {
            Value::Init => ValueType::Init,
            Value::Str(..) => ValueType::Str,
            Value::ZSet(..) => ValueType::ZSet,
        }
    }
}

// Key-value for the hash table
#[derive(Debug)]
pub struct Entry {
    link: LinkedListLink,
    hcode: u64,  // Keep the hash for performance
    key: String,
    value: Value,

    // for TTL: if None, entry is not in heap
    heap_idx: Option<usize>,

}

impl Entry {
    fn new(key: String, value: Value) -> Self {
        let hcode = hash_std(key.as_bytes());
        Self {
            link: LinkedListLink::new(),
            hcode,
            key,
            value,
            heap_idx: None,
        }
    }

    fn new_string(key: String, str_value: String) -> Self {
        Self::new(key, Value::Str(str_value))
    }
    
    fn new_zset(key: String, zset: ZSet) -> Self {
        Self::new(key, Value::ZSet(zset))
    }

}

fn entry_del(key: &str) {
    with_global_data(|g_data| {
        if let Some(entry) = g_data.db.delete_entry_and_return(key) {
            // Remove from TTL heap
            if let Some(heap_idx) = entry.heap_idx {
                if heap_idx < g_data.heap.len() {
                    heap_delete(&mut g_data.heap, heap_idx);
                }
            }
            
            let set_size = match &entry.value {
                Value::ZSet(zset) => zset.name_to_node.len(),
                _ => 0,
            };
            
            if set_size > K_LARGE_CONTAINER_SIZE {
                println!("Large ZSet detected ({} items), scheduling async cleanup", set_size);
                
                // Submit async work (entry drops here, doing the real cleanup)
                g_data.thread_pool.submit(move || {
                    // Simulate expensive cleanup work
                    std::thread::sleep(std::time::Duration::from_millis(10));
                    println!("Background: Completed simulated ZSet cleanup");
                });
            }
            // entry drops here, automatically cleaning up the ZSet
        }
    });
}

fn entry_set_ttl(entry_ref: Arc<Mutex<Entry>>, ttl_ms: i64, heap: &mut Vec<HeapItem>) {
    let mut entry = entry_ref.lock().unwrap();
    
    if ttl_ms < 0 {
        if let Some(idx) = entry.heap_idx {
            heap_delete(heap, idx);
            entry.heap_idx = None;
        }
    } else {
        let expire_at = get_monotonic_time_ms() + ttl_ms as u64;
        let item = HeapItem::new(expire_at, entry_ref.clone());
        heap_upsert(heap, &mut entry.heap_idx, item);
    }
}

fn heap_upsert(heap: &mut Vec<HeapItem>, heap_idx: &mut Option<usize>, item: HeapItem) {
    match *heap_idx {
        Some(pos) if pos < heap.len() => {
            // Update existing item
            heap[pos] = item;
            heap_update(heap, pos);
        }
        _ => {
            // Add new item
            let pos = heap.len();
            heap.push(item);
            *heap_idx = Some(pos);
            heap_update(heap, pos);
        }
    }
}

// Adapter for Entry to use in Intrusive Collections
intrusive_adapter!(pub EntryAdapter = Box<Entry>: Entry { link: LinkedListLink });


#[derive(Debug)]
pub struct HNode {
    link: LinkedListLink,
    hcode: u64,
} 


impl HNode {
    fn new(hcode: u64) -> Self {
        Self {
            link: LinkedListLink::new(),
            hcode,
        }
    }
}
    

//Adapter for HNode to use in Intrusive Collections
intrusive_adapter!(pub HNodeAdapter = Box<HNode>: HNode { link: LinkedListLink });

//fixed-size hash table
#[derive(Debug)]
pub struct HashTable {
    tab: Vec<LinkedList<EntryAdapter>>,
    mask: usize,
    size: usize,
}

impl HashTable {
    fn new(n_buckets: usize) -> Self {
        assert!(n_buckets.is_power_of_two());

        let tab = (0..n_buckets)
            .map(|_| LinkedList::new(EntryAdapter::new()))
            .collect::<Vec<_>>();

        Self {
            tab,
            mask: n_buckets - 1,
            size: 0,
        }
    }


    fn iter(&self) -> impl Iterator<Item = &Entry> {
    self.tab
        .iter()
        .flat_map(|bucket| bucket.iter())
    }

}


fn trigger_rehashing(hmap: &mut HMap) {
    let new_capacity = (hmap.newer.mask + 1) * 2;
    let old_table = std::mem::replace(&mut hmap.newer, HashTable::new(new_capacity));
    hmap.older = Some(old_table);
    hmap.migrate_pos = 0;
}


// Resizable Hash Tables 
#[derive(Debug)]
pub struct HMap {
    newer: HashTable,
    older: Option<HashTable>,
    migrate_pos: usize, 
}

impl HMap {
    fn new(initial_capacity: usize) -> Self {
        Self {
            newer: HashTable::new(initial_capacity),
            older: None,
            migrate_pos: 0,
        }
    }

    fn lookup_entry(&self, key: &str) -> Option<&Entry> {
        let eq = |entry: &Entry, probe: &str| -> bool {
            entry.key == probe
        };
        self.lookup(key, eq)
    }

    fn set(&mut self, key: String, value: String) {
        let entry = Box::new(Entry::new_string(key, value));  // Use new_string helper
        self.insert(entry);
    }

    pub fn lookup<F>(&self, key: &str, eq: F) -> Option<&Entry>
    where
        F: Fn(&Entry, &str) -> bool + Copy,
    {
        if let Some(hit) = hash_lookup(&self.newer, key, eq) {
            return Some(hit);
        }
        if let Some(ref older_table) = self.older {
            return hash_lookup(older_table, key, eq);
        }
        None
    }


    pub fn insert(&mut self, entry: Box<Entry>) {
        // check if hash map is initialised
        if self.newer.tab.is_empty() {
            self.newer = HashTable::new(4);
        }
 
        // insert into newer table
        insert_hash(&mut self.newer, entry);
 
        // rehashing check
        if self.older.is_none() {
            let capacity = self.newer.mask + 1;
            let threshold = capacity * K_MAX_LOAD_FACTOR;
     
            if self.newer.size >= threshold {
                trigger_rehashing(self);
            }
        }
 
        // migrate a little
        self.maybe_migrate();
    }

    pub fn delete<F>(&mut self, key: &str, eq: F) -> Option<Box<Entry>>
    where
        F: Fn(&Entry, &str) -> bool + Copy,
    {
        if let Some(mut cursor) = hash_lookup_cursor(&mut self.newer, key, eq) {
            let node = cursor.remove();
            if node.is_some() {
                self.newer.size -= 1;
            }
            return node;
        }

        if let Some(ref mut older_table) = self.older {
            if let Some(mut cursor) = hash_lookup_cursor(older_table, key, eq) {
                let node = cursor.remove();
                if node.is_some() {
                    older_table.size -= 1;
                }
                return node;
            }
        }

        None
    }

    pub fn hashmap_rehashing(&mut self) {
        let mut nwork = 0;

        while nwork < K_REHASHING_WORK && self.older.as_ref().map_or(0, |o| o.size) > 0 {
            if let Some(older) = &mut self.older {
                if self.migrate_pos >= older.tab.len() {
                    break;
                }

                if older.tab[self.migrate_pos].is_empty(){
                    self.migrate_pos += 1;
                    continue;
                }

               let mut cursor = older.tab[self.migrate_pos].front_mut();

                if let Some(entry) = cursor.remove() {
                    insert_hash(&mut self.newer, entry);
                    nwork += 1;
                }
            }
        }

        if let Some(older) = &self.older {
            if older.size == 0 {
                self.older = None;
            }
        }
    }
    
    #[allow(dead_code)]
    fn is_migrating(&self) -> bool {
        self.older.is_some()
    }

    fn size(&self) -> usize {
        let newer_size = self.newer.size;
        let older_size = self.older.as_ref().map_or(0, |h| h.size);
        newer_size + older_size
    }

    fn iter(&self) -> impl Iterator<Item = &Entry> + '_ {
        let newer_iter = self.newer.iter();
        let older_iter = self.older.as_ref().map(|h| h.iter()).into_iter().flatten();
        newer_iter.chain(older_iter)
    }


    pub fn delete_entry(&mut self, key: &str) -> bool {
        let eq = |entry: &Entry, probe: &str| -> bool {
            entry.key == probe
        };
        self.delete(key, eq).is_some()
    }

    pub fn delete_entry_and_return(&mut self, key: &str) -> Option<Box<Entry>> {
        let eq = |entry: &Entry, probe: &str| -> bool {
            entry.key == probe
        };
        self.delete(key, eq)
    }

    pub fn maybe_migrate(&mut self) {
        if self.older.is_some() {
            self.hashmap_rehashing();
        }
    }

}

impl Default for HMap {
    fn default() -> Self {
        Self::new(16)
    }
}


//Sorted Set //
#[derive(Debug, Default)]
struct ZSet {
    root: Option<Arc<Mutex<ZNode>>>, // AVL root
    name_to_node: HashMap<String, Arc<Mutex<ZNode>>> // index by name
}

impl ZSet {
    fn new() -> Self {
        Self {
            root: None,
            name_to_node: HashMap::new(),
        }
    }

    fn tree_insert(&mut self, node: Arc<Mutex<ZNode>>) {
        znode_insert(&mut self.root, node);
    }

    fn insert(&mut self, score: f64, name: String) -> bool {
        // Check if node already exists
        if let Some(existing_node) = self.lookup(&name) {
            self.zset_update(&existing_node, score);
            return false; // Updated existing
        }
        
        // Create new node
        let znode = ZNode::new(score, name.clone());
        
        // Insert into hash map (name -> node reference)
        self.name_to_node.insert(name, znode.clone());
        
        // Insert into tree
        self.tree_insert(znode);
        
        true // Inserted new
    }

    fn zset_update(&mut self, node: &Arc<Mutex<ZNode>>, new_score: f64) {
        // 1. Detach: remove node from tree
        self.root = znode_delete(self.root.clone(), node);

        // 2. Reset links so it’s clean for reinsert
        {
            let mut n = node.lock().unwrap();
            n.tree_left = None;
            n.tree_right = None;
            n.tree_parent = None;
            n.score = new_score;
        }

        // 3. Re-insert into AVL
        self.tree_insert(Arc::clone(node));
    }


    
    fn lookup(&self, name: &str) -> Option<Arc<Mutex<ZNode>>> {
        // First check the hash map for quick name-based lookup
        self.name_to_node.get(name).cloned()
    }

    fn lookup_by_score(&self, score: f64, name: &str) -> Option<Arc<Mutex<ZNode>>> {
        znode_search(&self.root, score, name)
    }


    fn delete(&mut self, node: &Arc<Mutex<ZNode>>) {
        let name = node.lock().unwrap().name.clone();
    
        // Remove from hash map
        assert!(
            self.name_to_node.remove(&name).is_some(),
            "Tried to delete non-existent node"
        );
    
        // Remove from tree
        self.root = znode_delete(self.root.clone(), node);
    }

    fn zset_seekge(
        &self,
        score: f64,
        name: &str,
        ) -> Option<Arc<Mutex<ZNode>>> {
        let mut candidate: Option<Arc<Mutex<ZNode>>> = None;
        let mut current = self.root.clone();

        while let Some(node_rc) = current {
            let node_ref = node_rc.lock().unwrap();

            // Compare (score, name) with current node
            let cmp = (OrderedFloat(node_ref.score), node_ref.name.as_str())
                .cmp(&(OrderedFloat(score), name));

            if cmp == std::cmp::Ordering::Less {
                // node < key → go right
                current = node_ref.tree_right.clone();
            } else {
                // node >= key → record candidate, go left
                candidate = Some(node_rc.clone());
                current = node_ref.tree_left.clone();
            }
        }

        candidate
    }



}

#[derive(Debug)]
struct ZNode {
    // AVL intrusive fields
    tree_parent: Option<Arc<Mutex<ZNode>>>,
    tree_left: Option<Arc<Mutex<ZNode>>>,
    tree_right: Option<Arc<Mutex<ZNode>>>,
    tree_height: u32,
    tree_count: u32,

    // Data
    score: f64,
    len: usize,
    name: String,
}

impl ZNode {
    fn new(score: f64, name: String) -> Arc<Mutex<Self>> {
        let len = name.len();
        Arc::new(Mutex::new(Self {
            tree_parent: None,
            tree_left: None,
            tree_right: None,
            tree_height: 1,
            tree_count: 1,
            score,
            len,
            name,
        }))
    }
}

// Option A: Implement Ord for ZNode so it works with AvlNode<ZNode>
impl Ord for ZNode {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.score.partial_cmp(&other.score) {
            Some(Ordering::Equal) => self.name.cmp(&other.name), // tie-break by name
            Some(ord) => ord,
            None => Ordering::Equal, // handle NaN
        }
    }
}

impl PartialOrd for ZNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for ZNode {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score && self.name == other.name
    }
}

impl Eq for ZNode {}

fn znode_insert(root: &mut Option<Arc<Mutex<ZNode>>>, new_node: Arc<Mutex<ZNode>>) {
    let mut current = root.clone();
    let mut parent: Option<Arc<Mutex<ZNode>>> = None;
    let mut is_left_child = false;
    
    // Find insertion point using (score, name) ordering
    while let Some(node) = current {
        parent = Some(Arc::clone(&node));
        
        let cmp = {
            let new_ref = new_node.lock().unwrap();
            let curr_ref = node.lock().unwrap();
            (OrderedFloat(new_ref.score), &new_ref.name)
                .cmp(&(OrderedFloat(curr_ref.score), &curr_ref.name))
        };
        
        match cmp {
            std::cmp::Ordering::Less => {
                current = node.lock().unwrap().tree_left.clone();
                is_left_child = true;
            }
            std::cmp::Ordering::Greater => {
                current = node.lock().unwrap().tree_right.clone();
                is_left_child = false;
            }
            std::cmp::Ordering::Equal => {
                // Duplicate (score, name) - this shouldn't happen in a sorted set
                // But if it does, we can replace or ignore. Let's ignore for now.
                return;
            }
        }
    }
    
    // Link new node to parent
    new_node.lock().unwrap().tree_parent = parent.clone();
    
    if let Some(parent_node) = parent {
        if is_left_child {
            parent_node.lock().unwrap().tree_left = Some(Arc::clone(&new_node));
        } else {
            parent_node.lock().unwrap().tree_right = Some(Arc::clone(&new_node));
        }
        // Rebalance from new node up to root
        *root = Some(znode_fix(new_node));
    } else {
        // This is the first node (root)
        *root = Some(new_node);
    }
}

fn znode_search(
    root: &Option<Arc<Mutex<ZNode>>>, 
    score: f64, 
    name: &str
) -> Option<Arc<Mutex<ZNode>>> {
    let mut current = root.clone();
    
    while let Some(node) = current {
        let cmp = {
            let node_ref = node.lock().unwrap();
            (OrderedFloat(score), name)
                .cmp(&(OrderedFloat(node_ref.score), node_ref.name.as_str()))
        };
        
        match cmp {
            std::cmp::Ordering::Less => {
                current = node.lock().unwrap().tree_left.clone();
            }
            std::cmp::Ordering::Greater => {
                current = node.lock().unwrap().tree_right.clone();
            }
            std::cmp::Ordering::Equal => {
                return Some(node);
            }
        }
    }
    
    None
}

fn znode_delete(
    root: Option<Arc<Mutex<ZNode>>>, 
    target: &Arc<Mutex<ZNode>>
) -> Option<Arc<Mutex<ZNode>>> {
    // This is similar to your existing avl_del but for ZNode
    
    // Easy case: 0 or 1 child
    let (has_left, has_right) = {
        let target_ref = target.lock().unwrap();
        (target_ref.tree_left.is_some(), target_ref.tree_right.is_some())
    };
    
    if !has_left || !has_right {
        return znode_del_easy(root, target);
    }

    // Hard case: find successor (leftmost node in right subtree)
    let mut victim = {
        let target_ref = target.lock().unwrap();
        target_ref.tree_right.clone().expect("Right child must exist")
    };

    loop {
        let next = {
            let v_ref = victim.lock().unwrap();
            v_ref.tree_left.clone()
        };
        match next {
            Some(left_child) => victim = left_child,
            None => break,
        }
    }

    // Swap the data (score, name) instead of moving nodes
    let victim_score = victim.lock().unwrap().score;
    let victim_name = victim.lock().unwrap().name.clone();
    let victim_len = victim.lock().unwrap().len;
    
    let target_score = target.lock().unwrap().score;
    let target_name = target.lock().unwrap().name.clone();
    let target_len = target.lock().unwrap().len;

    // Swap the data
    {
        let mut victim_mut = victim.lock().unwrap();
        victim_mut.score = target_score;
        victim_mut.name = target_name;
        victim_mut.len = target_len;
    }
    
    {
        let mut target_mut = target.lock().unwrap();
        target_mut.score = victim_score;
        target_mut.name = victim_name;
        target_mut.len = victim_len;
    }

    // Now delete the victim (which has at most 1 child)
    znode_del_easy(root, &victim)
}

fn znode_del_easy(
    root: Option<Arc<Mutex<ZNode>>>, 
    target: &Arc<Mutex<ZNode>>
) -> Option<Arc<Mutex<ZNode>>> {
    // Precondition: target has at most 1 child
    {
        let target_ref = target.lock().unwrap();
        assert!(
            !(target_ref.tree_left.is_some() && target_ref.tree_right.is_some()),
            "Node must have at most 1 child for easy deletion"
        );
    }

    // Extract child and parent
    let child = {
        let target_ref = target.lock().unwrap();
        target_ref.tree_left.clone().or_else(|| target_ref.tree_right.clone())
    };
    let parent = target.lock().unwrap().tree_parent.clone();

    // Update child's parent pointer
    if let Some(ref child_ref) = child {
        child_ref.lock().unwrap().tree_parent = parent.as_ref().map(Arc::clone);
    }

    // If target is root, return new root (child or None)
    if parent.is_none() {
        return child;
    }

    // Attach child to the correct side of parent
    let parent_node = parent.unwrap();
    let is_left_child = {
        let parent_ref = parent_node.lock().unwrap();
        parent_ref.tree_left.as_ref()
            .map_or(false, |left| Arc::ptr_eq(left, target))
    };

    if is_left_child {
        parent_node.lock().unwrap().tree_left = child;
    } else {
        parent_node.lock().unwrap().tree_right = child;
    }

    // Rebalance from parent upward
    Some(znode_fix(parent_node))
}

fn znode_update(node: &Arc<Mutex<ZNode>>) {
    let (left_height, right_height, left_count, right_count) = {
        let borrowed = node.lock().unwrap();
        (
            znode_height(&borrowed.tree_left),      // Fixed function name
            znode_height(&borrowed.tree_right),     // Fixed function name  
            avl_count(borrowed.tree_left.clone()),  // Fixed function name
            avl_count(borrowed.tree_right.clone()), // Fixed function name
        )
    };
    
    let mut borrowed = node.lock().unwrap();
    borrowed.tree_height = 1 + std::cmp::max(left_height, right_height);
    borrowed.tree_count = 1 + left_count + right_count;  // Fixed field name
}

fn znode_height(node: &Option<Arc<Mutex<ZNode>>>) -> u32 {
    node.as_ref().map_or(0, |rc| rc.lock().unwrap().tree_height)
}

fn znode_rot_right(node: Arc<Mutex<ZNode>>) -> Arc<Mutex<ZNode>> {
    let pivot = {
        let nb = node.lock().unwrap();
        nb.tree_left.clone().expect("Left child must exist for right rotation")
    };

    let pivot_right = pivot.lock().unwrap().tree_right.clone();

    {
        let mut node_mut = node.lock().unwrap();
        node_mut.tree_left = pivot_right.clone();
    }
    if let Some(pr) = &pivot_right {
        pr.lock().unwrap().tree_parent = Some(node.clone());
    }

    let parent = node.lock().unwrap().tree_parent.clone();
    pivot.lock().unwrap().tree_parent = parent.clone();

    if let Some(p) = parent {
        let mut p_mut = p.lock().unwrap();
        if p_mut.tree_left.as_ref().map_or(false, |left| Arc::ptr_eq(left, &node)) {
            p_mut.tree_left = Some(pivot.clone());
        } else if p_mut.tree_right.as_ref().map_or(false, |right| Arc::ptr_eq(right, &node)) {
            p_mut.tree_right = Some(pivot.clone());
        }
    }

    {
        let mut pivot_mut = pivot.lock().unwrap();
        pivot_mut.tree_right = Some(node.clone());
    }
    node.lock().unwrap().tree_parent = Some(pivot.clone());

    znode_update(&node);
    znode_update(&pivot);

    pivot
}

fn znode_rot_left(node: Arc<Mutex<ZNode>>) -> Arc<Mutex<ZNode>> {
    let pivot = {
        let nb = node.lock().unwrap();
        nb.tree_right.clone().expect("Right child must exist for left rotation")
    };

    let inner = pivot.lock().unwrap().tree_left.clone();

    {
        let mut node_mut = node.lock().unwrap();
        node_mut.tree_right = inner.clone();
    }
    if let Some(inner_node) = &inner {
        inner_node.lock().unwrap().tree_parent = Some(node.clone());
    }

    let parent = node.lock().unwrap().tree_parent.clone();
    pivot.lock().unwrap().tree_parent = parent.clone();

    if let Some(p) = parent {
        let mut p_mut = p.lock().unwrap();
        if p_mut.tree_left.as_ref().map_or(false, |left| Arc::ptr_eq(left, &node)) {
            p_mut.tree_left = Some(pivot.clone());
        } else if p_mut.tree_right.as_ref().map_or(false, |right| Arc::ptr_eq(right, &node)) {
            p_mut.tree_right = Some(pivot.clone());
        }
    }

    {
        let mut pivot_mut = pivot.lock().unwrap();
        pivot_mut.tree_left = Some(node.clone());
    }
    node.lock().unwrap().tree_parent = Some(pivot.clone());

    znode_update(&node);
    znode_update(&pivot);

    pivot
}

fn znode_fix_left(node: Arc<Mutex<ZNode>>) -> Arc<Mutex<ZNode>> {
    let needs_double = {
        let nb = node.lock().unwrap();
        let left = nb.tree_left.clone().expect("Left child must exist");
        let lh = znode_height(&left.lock().unwrap().tree_left);
        let rh = znode_height(&left.lock().unwrap().tree_right);
        lh < rh
    };

    if needs_double {
        let left_child = node.lock().unwrap().tree_left.clone().unwrap();
        let new_left = znode_rot_left(left_child);
        {
            let mut node_mut = node.lock().unwrap();
            node_mut.tree_left = Some(new_left.clone());
        }
        new_left.lock().unwrap().tree_parent = Some(node.clone());
    }

    znode_rot_right(node)
}

fn znode_fix_right(node: Arc<Mutex<ZNode>>) -> Arc<Mutex<ZNode>> {
    let needs_double = {
        let nb = node.lock().unwrap();
        let right = nb.tree_right.clone().expect("Right child must exist");
        let rl = znode_height(&right.lock().unwrap().tree_left);
        let rr = znode_height(&right.lock().unwrap().tree_right);
        rr < rl
    };

    if needs_double {
        let right_child = node.lock().unwrap().tree_right.clone().unwrap();
        let new_right = znode_rot_right(right_child);
        {
            let mut node_mut = node.lock().unwrap();
            node_mut.tree_right = Some(new_right.clone());
        }
        new_right.lock().unwrap().tree_parent = Some(node.clone());
    }

    znode_rot_left(node)
}

fn znode_fix(mut node: Arc<Mutex<ZNode>>) -> Arc<Mutex<ZNode>> {
    loop {
        let parent_opt = node.lock().unwrap().tree_parent.clone(); // Fixed
        
        znode_update(&node); // Fixed function name
        
        let (left_height, right_height) = {
            let nb = node.lock().unwrap();
            (znode_height(&nb.tree_left), znode_height(&nb.tree_right)) // Fixed
        };
        
        let mut fixed = Arc::clone(&node);
        if left_height == right_height + 2 {
            fixed = znode_fix_left(node); // Fixed function name
        } else if left_height + 2 == right_height {
            fixed = znode_fix_right(node); // Fixed function name
        }
        
        if let Some(parent) = parent_opt {
            let is_left_child = {
                let parent_ref = parent.lock().unwrap();
                parent_ref.tree_left.as_ref().map(|l| Arc::ptr_eq(l, &fixed)).unwrap_or(false) // Fixed
            };
            
            if is_left_child {
                parent.lock().unwrap().tree_left = Some(Arc::clone(&fixed));
            } else {
                parent.lock().unwrap().tree_right = Some(Arc::clone(&fixed));
            }
            
            node = parent;
        } else {
            return fixed;
        }
    }
}

fn znode_offset(
    node: Option<Arc<Mutex<ZNode>>>, 
    offset: i64
) -> Option<Arc<Mutex<ZNode>>> {
    match node {
        Some(znode_rc) => {
            // Just delegate to avl_offset
            avl_offset(Some(znode_rc.clone()), offset)
        }
        None => None,
    }
}


#[derive(Debug, Clone)]
struct HeapItem {
    value: u64,
    entry_ref: Arc<Mutex<Entry>>,
}

impl HeapItem {
    fn new(value: u64, entry_ref: Arc<Mutex<Entry>>) -> Self {
        Self { value, entry_ref }
    }
}

fn heap_left(i: usize) -> usize {
    i * 2 + 1
}

fn heap_right(i: usize) -> usize {
    i * 2 + 2
}

fn heap_parent(i: usize) -> usize {
    (i - 1) / 2
}

fn heap_up(items: &mut [HeapItem], mut pos: usize) {
    let temp = items[pos].clone();

    while pos > 0 && items[heap_parent(pos)].value > temp.value {
        items[pos] = items[heap_parent(pos)].clone();
        // Update backlink for moved item
        items[pos].entry_ref.lock().unwrap().heap_idx = Some(pos);
        pos = heap_parent(pos);
    }

    items[pos] = temp;
    items[pos].entry_ref.lock().unwrap().heap_idx = Some(pos);
}

fn heap_down(items: &mut [HeapItem], mut pos: usize) {
    let temp = items[pos].clone();
    let len = items.len();

    loop {
        let left = heap_left(pos);
        let right = heap_right(pos);

        let mut min_pos = pos;
        let mut min_val = temp.value;

        if left < len && items[left].value < min_val {
            min_pos = left;
            min_val = items[left].value;
        }
        if right < len && items[right].value < min_val {
            min_pos = right;
        }

        if min_pos == pos {
            break;
        }

        items[pos] = items[min_pos].clone();
        // Update backlink for moved item
        items[pos].entry_ref.lock().unwrap().heap_idx = Some(pos);
        pos = min_pos;
    }

    items[pos] = temp;
    items[pos].entry_ref.lock().unwrap().heap_idx = Some(pos);
}


fn heap_update(items: &mut [HeapItem], pos: usize) {
    if pos > 0 && items[heap_parent(pos)].value > items[pos].value {
        heap_up(items, pos);
    } else {
        heap_down(items, pos); // len handled inside
    }
}

fn heap_delete(items: &mut Vec<HeapItem>, pos: usize) {
    if pos >= items.len() {
        return;
    }
    
    // Clear the backlink in the deleted item
    items[pos].entry_ref.lock().unwrap().heap_idx = None;
    
    if let Some(last_item) = items.pop() {
        if pos < items.len() {
            // Replace deleted item with the last element
            items[pos] = last_item;
            // Update backlink for moved item
            items[pos].entry_ref.lock().unwrap().heap_idx = Some(pos);
            // Restore heap property
            heap_update(items, pos);
        }
    }
}

pub fn insert_hash(htab: &mut HashTable, entry: Box<Entry>) {
    let pos = (entry.hcode as usize) & htab.mask;
    htab.tab[pos].push_front(entry);
    htab.size += 1;
}

pub fn hash_lookup<'a, F>(
    table: &'a HashTable,
    key: &str,
    eq: F,
) -> Option<&'a Entry>
where
    F: Fn(&Entry, &str) -> bool + Copy,
{
    let bucket_index = (hash_std(key.as_bytes()) as usize) & table.mask;
    let bucket = &table.tab[bucket_index];

    let mut cursor = bucket.cursor();

    while !cursor.is_null() {
        let entry = cursor.get().unwrap();
        if eq(entry, key) {
            return Some(entry);
        }
        cursor.move_next();
    }

    None
}

pub fn hash_lookup_cursor<'a, F>(
    htab: &'a mut HashTable,
    key: &str,
    eq: F,
) -> Option<CursorMut<'a, EntryAdapter>>
where
    F: Fn(&Entry, &str) -> bool,
{
    if htab.tab.is_empty() {
        return None;
    }

    let hcode = hash_std(key.as_bytes());
    let pos = (hcode as usize) & htab.mask;

    let mut cur = htab.tab[pos].front_mut();

    while let Some(entry) = cur.get() {
        if entry.hcode == hcode && eq(entry, key) {
            return Some(cur);
        }
        cur.move_next();
    }
    None
}

pub fn hash_delete(
    htab: &mut HashTable,
    mut cursor: CursorMut<EntryAdapter>,
) -> Option<Box<Entry>> {
    let node = cursor.remove();
    if node.is_some() {
        htab.size -= 1;
    }
    node
}

// global data structure
#[derive(Debug)]
struct GData {
    db: HMap,
    fd2conn: HashMap<RawFd, Conn>,
    idle_list: Arc<Mutex<DList>>,
    heap: Vec<HeapItem>,
    thread_pool: ThreadPool,
    ttl_map: HashMap<String, usize>,
}

impl GData {
    fn new() -> Self {
        let idle_list = DList::new();
        // Initialize the idle list as circular
        {
            let mut idle_ref = idle_list.lock().unwrap();
            idle_ref.prev = Some(idle_list.clone());
            idle_ref.next = Some(idle_list.clone());
        }
        
        Self {
            db: HMap::default(),
            fd2conn: HashMap::new(),
            idle_list,
            heap: Vec::new(),
            thread_pool: ThreadPool::new(4),
            ttl_map: HashMap::new(),
        }
    }
}

impl std::fmt::Debug for ThreadPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ThreadPool")
            .field("num_threads", &self.threads.len())
            .field("queue_len", &{
                let (lock, _) = &*self.queue;
                lock.lock().unwrap().len()
            })
            .field("shutdown", &{
                *self.shutdown.lock().unwrap()
            })
            .finish()
    }
}

// Synchronous deletion (runs in current thread)
fn entry_del_sync(mut entry: Box<Entry>) {
    match &mut entry.value {
        Value::ZSet(zset) => {
            println!("Clearing ZSet with {} items", zset.name_to_node.len());
            
            // Clear the hash map (this is the expensive O(N) operation)
            zset.name_to_node.clear();
            
            // Clear the AVL tree root
            zset.root = None;
            
            // All Rc<RefCell<ZNode>> references should be dropped automatically
        }
        Value::Str(_) => {
            // Strings don't need special handling - just drop
        }
        Value::Init => {
            // Nothing to clean up
        }
    }
    // Entry drops here, freeing all memory
}

// Wrapper for thread pool (matches C pattern)
fn entry_del_async_wrapper(entry: Box<Entry>) {
    entry_del_sync(entry);
}


fn out_nil(buf: &mut Buffer) {
    buf.append(&[Tag::Nil as u8]);
}

fn out_str(buf: &mut Buffer, s: &str) {
    buf.append_u8(Tag::Str as u8);
    buf.append_u32(s.len() as u32);
    buf.append(s.as_bytes());
}

fn out_int(buf: &mut Buffer, val: i64) {
    buf.append_u8(Tag::Int as u8);
    buf.append_i64(val);
}

fn out_dbl(buf: &mut Buffer, val: f64) {
    buf.append(&[Tag::Dbl as u8]);
    buf.append(&val.to_le_bytes());
}

fn out_arr(buf: &mut Buffer, n: u32) {
    buf.append(&[Tag::Arr as u8]);
    buf.append(&n.to_le_bytes());
}

fn out_err(buf: &mut Buffer, msg: &str) {
    buf.append(&[Tag::Err as u8]);
    buf.append(&(msg.len() as u32).to_le_bytes());
    buf.append(msg.as_bytes());
}

fn do_keys(out: &mut Buffer) -> Result<(), &'static str> {
    with_global_data(|g_data| {
        let key_count = g_data.db.size();
        out_arr(out, key_count as u32);
        
        // Iterate and output each key
        for entry in g_data.db.iter() {
            out_str(out, &entry.key);
        }
    });
    
    Ok(())
    
}

// GET key
fn do_get(db: &HMap, cmd: &[String], out: &mut Buffer) -> Result<(), &'static str> {
    if cmd.len() < 2 {
        out_err(out, "GET requires a key");
        return Ok(());
    }

    let key = &cmd[1];

    match db.lookup_entry(key.as_str()) {
        None => {
            out_nil(out);
        }
        Some(entry) => {
            match &entry.value {
                Value::Str(string_value) => {
                    // Handle string values (your original logic)
                    if string_value.len() > K_MAX_MSG {
                        out_err(out, "value too large");
                        return Ok(());
                    }
                    out_str(out, string_value);
                }
                Value::ZSet(_zset) => {
                    // GET command doesn't work on sorted sets
                    out_err(out, "WRONGTYPE Operation against a key holding the wrong kind of value");
                }
                Value::Init => {
                    out_nil(out);
                }
            }
        }
    }

    Ok(())
}

fn do_set(cmd: &[String], out: &mut Buffer) -> Result<(), &'static str> {
    if cmd.len() < 3 {
        out_err(out, "SET requires key and value");
        return Err("SET requires key and value");
    }

    let key = cmd[1].clone();
    let value = cmd[2].clone();

    with_global_data(|g_data| {
        g_data.db.set(key, value);
        out_nil(out);  // SET returns nil on success
    });

    Ok(())
}

fn do_del(cmd: &[String], out: &mut Buffer) -> Result<(), &'static str> {
    if cmd.len() < 2 {
        out_err(out, "DEL requires at least one key");
        return Ok(());
    }

    let mut deleted_count = 0i64;

    // DEL can delete multiple keys: DEL key1 key2 key3
    for key in &cmd[1..] {
        with_global_data(|g_data| {
            if g_data.db.lookup_entry(key).is_some() {
                deleted_count += 1;
                // Don't increment here - do it outside the closure
            }
        });
        
        // Use new async-capable entry_del
        if deleted_count > 0 {
            entry_del(key);
        }
    }

    out_int(out, deleted_count);
    Ok(())
}

fn do_zquery(cmd: &[String], out: &mut Buffer) -> Result<(), &'static str> {
    if cmd.len() < 6 {
        out_err(out, "ZQUERY requires: key score name offset limit");
        return Ok(());
    }

    let key = &cmd[1];
    let score: f64 = cmd[2].parse().map_err(|_| "Invalid score")?;
    let name: &str = &cmd[3];
    let offset: i64 = cmd[4].parse().map_err(|_| "Invalid offset")?;
    let limit: usize = cmd[5].parse().map_err(|_| "Invalid limit")?;

    with_global_data(|g_data| {
        match g_data.db.lookup_entry(key) {
            Some(entry) => match &entry.value {
                Value::ZSet(zset) => {
                    let mut znode = zset.zset_seekge(score, name);

                    if let Some(node) = znode.clone() {
                        znode = znode_offset(Some(node), offset);
                    }

                    let ctx = out.out_begin_arr();
                    let mut n = 0i64; // <-- this was missing

                    while let Some(node) = znode {
                        if n >= limit as i64 * 2 {
                            break;
                        }

                        // Borrow and extract data first, then drop the borrow
                        let (name, score) = {
                            let node_ref = node.lock().unwrap();
                            (node_ref.name.clone(), node_ref.score)
                        };

                        out_str(out, &name);
                        out_dbl(out, score);
                        n += 2;

                        // Now safe to move node since borrow ended
                        znode = znode_offset(Some(node), 1);
                    }

                    out.out_end_arr(ctx, n as u32);

                }
                _ => out_err(out, "WRONGTYPE Operation against a key holding the wrong kind of value"),
            },
            None => out_nil(out),
        }
    });

    Ok(())
}

fn do_expire(cmd: &[String], out: &mut Buffer) -> Result<(), &'static str> {
    if cmd.len() < 3 {
        out_err(out, "EXPIRE requires key and seconds");
        return Ok(());
    }

    let ttl_seconds: i64 = match cmd[2].parse() {
        Ok(val) => val,
        Err(_) => {
            out_err(out, "Expected int64");
            return Ok(());
        }
    };
    
    let key = cmd[1].clone();
    
     with_global_data(|g_data| {
        // Use the remove-modify-insert pattern to set TTL
        if let Some(mut entry_box) = g_data.db.delete_entry_and_return(&key) {
            if ttl_seconds <= 0 {
                // Remove existing TTL
                if let Some(heap_idx) = entry_box.heap_idx {
                    if heap_idx < g_data.heap.len() {
                        heap_delete(&mut g_data.heap, heap_idx);
                    }
                }
                entry_box.heap_idx = None;
                out_int(out, 1);
            } else {
                // Set TTL
                let expire_at = get_monotonic_time_ms() + (ttl_seconds * 1000) as u64;
                let entry_ref = Arc::new(Mutex::new(Entry {
                    link: LinkedListLink::new(),
                    hcode: entry_box.hcode,
                    key: entry_box.key.clone(),
                    value: Value::Str(key.clone()), // Placeholder for heap
                    heap_idx: entry_box.heap_idx,
                }));
                let heap_item = HeapItem::new(expire_at, entry_ref);
                
                heap_upsert(&mut g_data.heap, &mut entry_box.heap_idx, heap_item);
                out_int(out, 1);
            }
            
            // Re-insert the entry
            g_data.db.insert(entry_box);
        } else {
            out_int(out, 0); // Key not found
        }
    });

    Ok(())
}

// TTL command - returns remaining TTL in seconds
fn do_ttl(cmd: &[String], out: &mut Buffer) -> Result<(), &'static str> {
    if cmd.len() < 2 {
        out_err(out, "TTL requires a key");
        return Ok(());
    }

    let key = &cmd[1];
    
    with_global_data(|g_data| {
        match g_data.db.lookup_entry(key) {
            Some(entry) => {
                if let Some(heap_idx) = entry.heap_idx {
                    if heap_idx < g_data.heap.len() {
                        let expire_at = g_data.heap[heap_idx].value;
                        let now_ms = get_monotonic_time_ms();
                        
                        if expire_at > now_ms {
                            let remaining_ms = expire_at - now_ms;
                            let remaining_seconds = (remaining_ms + 999) / 1000; // Round up
                            out_int(out, remaining_seconds as i64);
                        } else {
                            out_int(out, -2); // Key expired
                        }
                    } else {
                        out_int(out, -1); // No TTL set
                    }
                } else {
                    out_int(out, -1); // No TTL set
                }
            }
            None => {
                out_int(out, -2); // Key doesn't exist
            }
        }
    });

    Ok(())
}


fn do_persist(cmd: &[String], out: &mut Buffer) -> Result<(), &'static str> {
    if cmd.len() < 2 {
        out_err(out, "PERSIST requires a key");
        return Ok(());
    }

    let key = &cmd[1];
    
    with_global_data(|g_data| {
        match g_data.db.lookup_entry(key) {
            Some(entry) => {
                if let Some(heap_idx) = entry.heap_idx {
                    if heap_idx < g_data.heap.len() {
                        heap_delete(&mut g_data.heap, heap_idx);
                        out_int(out, 1); // TTL was removed
                    } else {
                        out_int(out, 0); // No TTL was set
                    }
                } else {
                    out_int(out, 0); // No TTL was set
                }
            }
            None => {
                out_int(out, 0); // Key doesn't exist
            }
        }
    });

    Ok(())
}

fn do_zadd(cmd: &[String], out: &mut Buffer) -> Result<(), &'static str> {
    if cmd.len() < 4 || (cmd.len() % 2) != 0 {
        out_err(out, "ZADD requires: key score member [score member ...]");
        return Ok(());
    }

    let key = &cmd[1];
    let mut added = 0;

    // Parse and validate all score-member pairs first
    let mut pairs = Vec::new();
    let mut i = 2;
    while i + 1 < cmd.len() {
        match cmd[i].parse::<f64>() {
            Ok(score) => pairs.push((score, cmd[i + 1].clone())),
            Err(_) => {
                out_err(out, &format!("Invalid score: {}", cmd[i]));
                return Ok(());
            }
        }
        i += 2;
    }

    with_global_data(|g_data| {
        // Get or create ZSet
        let mut zset_entry = match g_data.db.delete_entry_and_return(key) {
            Some(entry) => match entry.value {
                Value::ZSet(_) => entry,
                _ => {
                    out_err(out, "WRONGTYPE Operation against a key holding the wrong kind of value");
                    return;
                }
            },
            None => Box::new(Entry::new_zset(key.clone(), ZSet::new())),
        };

        // Add all pairs
        if let Value::ZSet(ref mut zset) = zset_entry.value {
            for (score, member) in pairs {
                if zset.insert(score, member) {
                    added += 1;
                }
            }
        }

        // Re-insert the entry
        g_data.db.insert(zset_entry);
    });

    out_int(out, added);
    Ok(())
}

fn do_zrem(cmd: &[String], out: &mut Buffer) -> Result<(), &'static str> {
    if cmd.len() < 3 {
        out_err(out, "ZREM requires: key member [member ...]");
        return Ok(());
    }

    let key = &cmd[1];
    let members = &cmd[2..];
    let mut removed = 0;

    with_global_data(|g_data| {
        if let Some(mut zset_entry) = g_data.db.delete_entry_and_return(key) {
            if let Value::ZSet(ref mut zset) = zset_entry.value {
                for member in members {
                    if let Some(node) = zset.lookup(member) {
                        zset.delete(&node);
                        removed += 1;
                    }
                }

                // Re-insert if ZSet is not empty
                if !zset.name_to_node.is_empty() {
                    g_data.db.insert(zset_entry);
                }
                // If empty, let it drop (effectively deleting the key)
            } else {
                // Wrong type - re-insert and error
                g_data.db.insert(zset_entry);
                out_err(out, "WRONGTYPE Operation against a key holding the wrong kind of value");
                return;
            }
        }
    });

    out_int(out, removed);
    Ok(())
}

fn with_global_data<F, R>(f: F) -> R
where
    F: FnOnce(&mut GData) -> R,
{
    let data = GLOBAL_DATA.get_or_init(|| {
        let idle_list = DList::new();
        dlist_init(idle_list.clone());
        Mutex::new(GData {
            db: HMap::default(),
            fd2conn: HashMap::new(),
            idle_list,
            heap: Vec::new(),
            thread_pool: ThreadPool::new(4),
            ttl_map: HashMap::new(),
        })
    });
    
    let mut guard = data.lock().unwrap();
    f(&mut *guard)
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
enum Tag {
    Nil = 0,    // nil
    Err = 1,    // error code + msg
    Str = 2,    // string
    Int = 3,    // int64
    Dbl = 4,    // double
    Arr = 5,    // array
}

impl Tag {
    /// Create an empty RedisValue of this type
    /// Useful for protocol deserialization scaffolding
    fn empty_value(&self) -> RedisValue {
        match self {
            Tag::Nil => RedisValue::Nil,
            Tag::Err => RedisValue::Err(String::new()),
            Tag::Str => RedisValue::Str(String::new()),
            Tag::Int => RedisValue::Int(0),
            Tag::Dbl => RedisValue::Dbl(0.0),
            Tag::Arr => RedisValue::Arr(Vec::new()),
        }
    }
    
    /// Create a RedisValue with actual data
    /// Will be useful when parsing protocol messages
    fn with_data(&self, data: &[u8]) -> Result<RedisValue, String> {
        match self {
            Tag::Nil => Ok(RedisValue::Nil),
            Tag::Err => Ok(RedisValue::Err(String::from_utf8_lossy(data).to_string())),
            Tag::Str => Ok(RedisValue::Str(String::from_utf8_lossy(data).to_string())),
            Tag::Int => {
                let s = String::from_utf8_lossy(data);
                s.parse::<i64>()
                    .map(RedisValue::Int)
                    .map_err(|_| "Invalid integer".to_string())
            }
            Tag::Dbl => {
                let s = String::from_utf8_lossy(data);
                s.parse::<f64>()
                    .map(RedisValue::Dbl)
                    .map_err(|_| "Invalid double".to_string())
            }
            Tag::Arr => {
                // Arrays need special parsing - just create empty for now
                Ok(RedisValue::Arr(Vec::new()))
            }
        }
    }
}

// Redis value that can hold any data type
#[derive(Debug, Clone)]
enum RedisValue {
    Nil,
    Err(String),                    // Error message
    Str(String),                    // String value
    Int(i64),                       // Integer value
    Dbl(f64),                       // Double value
    Arr(Vec<RedisValue>),          // Array of values (can be nested)
}

impl RedisValue {
    fn tag(&self) -> Tag {
        match self {
            RedisValue::Nil => Tag::Nil,
            RedisValue::Err(_) => Tag::Err,
            RedisValue::Str(_) => Tag::Str,
            RedisValue::Int(_) => Tag::Int,
            RedisValue::Dbl(_) => Tag::Dbl,
            RedisValue::Arr(_) => Tag::Arr,
        }
    }
}

#[derive(Debug)]
struct Conn{
    socket: Socket,

    //application intention, for the event loop
    want_read: bool,
    want_write: bool,
    want_close: bool,

    //buffered input and output
    incoming: Buffer,
    outgoing: Buffer,

    last_active_ms: u64,
    idle_node: Arc<Mutex<DList>>


}

impl Conn {
    fn new(socket: Socket) -> Self {
        Self {
            socket,
            want_read: true,
            want_write: false,
            want_close: false,
            incoming: Buffer::new(),
            outgoing: Buffer::new(),
            last_active_ms: get_monotonic_time_ms(),
            idle_node: DList::new(),
        }
    }
}

fn events_from_conn(conn: &Conn) -> PollFlags {
    let mut events = PollFlags::POLLERR;
    if conn.want_read {
        events |= PollFlags::POLLIN;
    }
    if conn.want_write {
        events |= PollFlags::POLLOUT;
    }
    events
}

fn run_server() -> io::Result<()> {
    let server_socket = Socket::new(Domain::IPV6, Type::STREAM, Some(Protocol::TCP))?;
    server_socket.set_only_v6(false)?;
    server_socket.set_reuse_address(true)?;
    let addr: SocketAddr = "[::]:1234".parse().unwrap();
    let sockaddr = SockAddr::from(addr);
    server_socket.bind(&sockaddr)?;
    server_socket.set_nonblocking(true)?;
    server_socket.listen(BACKLOG)?;
    println!("Server listening on {:?}", addr);

    let running = true;

    while running {
        let mut poll_fds = Vec::new();
        poll_fds.push(PollFd::new(&server_socket, PollFlags::POLLIN));

        let client_entries: Vec<(RawFd, Socket, PollFlags)> = with_global_data(|g_data| {
            g_data.fd2conn
                .iter()
                .map(|(&fd, conn)| {
                    let sock_clone = conn.socket.try_clone().unwrap();
                    (fd, sock_clone, events_from_conn(conn))
                })
                .collect()
        });

        for (_, socket, events) in &client_entries {
            poll_fds.push(PollFd::new(socket, *events));
        }

        let timeout_ms = next_timer_ms();
        match poll(&mut poll_fds, timeout_ms) {
            Ok(_) => {
                let server_fd = server_socket.as_raw_fd();
                let mut to_remove = Vec::new(); // Store fds to remove after loop

                for poll_fd in &poll_fds {
                    let fd = poll_fd.as_fd().as_raw_fd();
                    let revents = poll_fd.revents().unwrap_or(PollFlags::empty());

                    if fd == server_fd && revents.contains(PollFlags::POLLIN) {
                        // Handle new connections
                        loop {
                            match server_socket.accept() {
                                Ok((client_socket, client_addr)) => {
                                    println!("Client connected: {:?}", client_addr);
                                    client_socket.set_nonblocking(true)?;
                                    let client_fd = client_socket.as_raw_fd();
                                    
                                    let conn = Conn::new(client_socket);
                                    
                                    with_global_data(|g_data| {
                                        dlist_insert_before(&g_data.idle_list, &conn.idle_node);
                                        g_data.fd2conn.insert(client_fd, conn);
                                    });
                                }
                                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                                Err(e) => {
                                    eprintln!("Accept error: {}", e);
                                    break;
                                }
                            }
                        }
                    } else if revents.contains(PollFlags::POLLIN) {
                        with_global_data(|g_data| {
                            if let Some(conn) = g_data.fd2conn.get_mut(&fd) {
                                if conn.want_read {
                                    match handle_read(conn) {
                                        Ok(()) => {}
                                        Err(_) => {
                                            println!("Client {} disconnected", fd);
                                            to_remove.push(fd);
                                        }
                                    }
                                }
                            }
                        });
                    } else if revents.contains(PollFlags::POLLOUT) {
                        with_global_data(|g_data| {
                            if let Some(conn) = g_data.fd2conn.get_mut(&fd) {
                                if conn.want_write && !conn.outgoing.is_empty() {
                                    match handle_write(conn) {
                                        Ok(()) => {}
                                        Err(_) => {
                                            println!("Client {} disconnected during write", fd);
                                            to_remove.push(fd);
                                        }
                                    }
                                }
                            }
                        });
                    }

                    // Check for connections that should be closed
                    with_global_data(|g_data| {
                        if let Some(conn) = g_data.fd2conn.get(&fd) {
                            if conn.want_close {
                                to_remove.push(fd);
                            }
                        }
                    });
                }

                // ADD THIS SECTION: Remove disconnected clients
                for fd in to_remove {
                    with_global_data(|g_data| {
                        if let Some(conn) = g_data.fd2conn.remove(&fd) {
                            // Remove from idle list
                            dlist_detach(conn.idle_node.clone());
                            println!("Cleaned up connection for fd: {}", fd);
                        }
                    });
                }

                // Process timers after handling all I/O events
                process_timers();
            }
            Err(e) => {
                eprintln!("Poll error: {}", e);
                break;
            }
        }
    }

    Ok(())
}


fn handle_read(conn: &mut Conn) -> io::Result<()> {
    // 1. Non-blocking read
    let mut buf = [0u8; 64 * 1024];
    match conn.socket.read(&mut buf) {
        Ok(0) => {
            // EOF: client closed
            conn.want_close = true;
            return Ok(());
        }
        Ok(n) => {
            // Append to incoming buffer
            conn.incoming.extend_from_slice(&buf[..n]);
        }
        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
            // No data yet, try again later
            return Ok(());
        }
        Err(e) => return Err(e),
    }

    // 2. Try to parse requests
    try_parse_request(conn)?;

    if !conn.outgoing.is_empty() {
        conn.want_read = false;   // Stop reading until we send response
        conn.want_write = true;   // Start writing the response

        match handle_write(conn) {
            Ok(()) => {}
            Err(e) => return Err(e),
        }
    }

    Ok(())


}

fn handle_write(conn: &mut Conn) -> io::Result<()> {
    assert!(!conn.outgoing.is_empty());

    match conn.socket.write(&conn.outgoing) {
        Ok(0) => {
            conn.want_close = true;
            return Err(io::Error::new(io::ErrorKind::WriteZero, "Socket closed"));
        }
        Ok(n) => {
            conn.outgoing.consume(n); // Remove written bytes

            if conn.outgoing.is_empty() {
                conn.want_write = false;
                conn.want_read = true;
            }

            println!("Wrote {} bytes, {} bytes remaining", n, conn.outgoing.len());
        }
        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
            return Ok(());
        }
        Err(e) => {
            conn.want_close = true;
            return Err(e);
        }
    }

    Ok(())
}


fn try_parse_request(conn: &mut Conn) -> io::Result<()> {
    loop {
        // 3. Need at least 4 bytes for header
        if conn.incoming.len() < 4 {
            break;
        }

        // Parse message length
        let len_bytes: [u8; 4] = conn.incoming[..4].try_into().unwrap();
        let msg_len = u32::from_le_bytes(len_bytes) as usize;

        // Protocol sanity check
        if msg_len > K_MAX_MSG {
            conn.want_close = true;
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Message too long"));
        }

        let total_len = 4 + msg_len;
        if conn.incoming.len() < total_len {
            // Not enough data yet, wait for next read
            break;
        }

        // Extract message body
        let message_data = conn.incoming[4..total_len].to_vec();
        println!("client says: {}", String::from_utf8_lossy(&message_data));

        // 4. Parse command and generate response
        let message_str = String::from_utf8_lossy(&message_data);
        let parts: Vec<String> = message_str.split_whitespace().map(|s| s.to_string()).collect();

        if !parts.is_empty() {
            // Begin response (reserve header space)
            let header_pos = conn.outgoing.response_begin();
    
            // Process the command
            match parts[0].to_uppercase().as_str() {
                "GET" => {
                    with_global_data(|g_data| {
                        do_get(&g_data.db, &parts, &mut conn.outgoing).unwrap();
                    });
                }
                "SET" => {
                    do_set(&parts, &mut conn.outgoing).unwrap();
                }
                "DEL" => {
                    do_del(&parts, &mut conn.outgoing).unwrap();
                }
                "KEYS" => {
                    do_keys(&mut conn.outgoing).unwrap();
                }
                "ZADD" => {
                    do_zadd(&parts, &mut conn.outgoing).unwrap();
                }
                "ZREM" => {
                    do_zrem(&parts, &mut conn.outgoing).unwrap();
                }
                "ZQUERY" => {
                    do_zquery(&parts, &mut conn.outgoing).unwrap();  // Add this line
                }
                "EXPIRE" => {
                    do_expire(&parts, &mut conn.outgoing).unwrap();
                }
                "TTL" => {
                    do_ttl(&parts, &mut conn.outgoing).unwrap();
                }
                "PERSIST" => {
                    do_persist(&parts, &mut conn.outgoing).unwrap();
                }
                _ => out_err(&mut conn.outgoing, "Unknown command"),
            }
            // End response (write actual size to header)
            conn.outgoing.response_end(header_pos);

        }
        // After the command processing block, add:
        conn.incoming.consume(total_len);
    }

    Ok(())
}



fn one_request<T: Read + Write>(socket: &mut T) -> io::Result<()> {
    let mut rbuf = [0u8; 4 + K_MAX_MSG];

    set_errno(Errno(0));

    // Read header
    if let Err(e) = read_full(socket, &mut rbuf[..4]) {
        if errno().0 == 0 {
            println!("EOF");
        } else {
            println!("read() error: {}", e);
        }
        return Err(e);
    }

    let len = u32::from_le_bytes(rbuf[..4].try_into().unwrap()) as usize;
    if len > K_MAX_MSG {
        eprintln!("too long");
        return Err(io::Error::new(io::ErrorKind::InvalidData, "too long"));
    }

    // Read body
    if let Err(e) = read_full(socket, &mut rbuf[4..4 + len]) {
        eprintln!("read() error: {}", e);
        return Err(e);
    }

    println!("client says: {}", String::from_utf8_lossy(&rbuf[4..4 + len]));

    // Prepare reply
    let reply = b"world";
    let mut wbuf = vec![0u8; 4 + reply.len()];
    wbuf[..4].copy_from_slice(&(reply.len() as u32).to_le_bytes());
    wbuf[4..].copy_from_slice(reply);

    // Send reply
    write_all(socket, &wbuf)?;
    Ok(())
}

// Read exactly `n` bytes into `buf`
fn read_full<T: Read>(socket: &mut T, mut buf: &mut [u8]) -> io::Result<()> {
    while !buf.is_empty() {
        match socket.read(buf) {
            Ok(0) => return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "connection closed")),
            Ok(rv) => {
                assert!(rv <= buf.len());
                buf = &mut buf[rv..]; // advance buffer
            }
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

// Write exactly all bytes from `buf`
fn write_all<T: Write>(socket: &mut T, mut buf: &[u8]) -> io::Result<()> {
    while !buf.is_empty() {
        match socket.write(buf) {
            Ok(0) => return Err(io::Error::new(io::ErrorKind::WriteZero, "failed to write to socket")),
            Ok(rv) => {
                assert!(rv <= buf.len());
                buf = &buf[rv..]; // advance buffer
            }
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/* Client Logic */
fn run_client() -> std::io::Result<()> {
    // Create socket
    let mut socket = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))?;

    // Server address: 127.0.0.1:1234
    let server_addr: SocketAddr = "127.0.0.1:1234".parse().unwrap();
    let sockaddr = SockAddr::from(server_addr);

    // Connect to server
    socket.connect(&sockaddr)?;

    // Prepare message with protocol header
    query(&mut socket, "hello1")?;
    query(&mut socket, "hello2")?;  
    query(&mut socket, "hello3")?;
    

    Ok(())
}

fn query<T: Read + Write>(socket: &mut T, text: &str) -> io::Result<()> {
    let len = text.len();
    if len > K_MAX_MSG {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "Message too long"));
    }

    // Prepare write buffer: 4-byte length header + body
    let mut wbuf = [0u8; 4 + K_MAX_MSG];
    wbuf[..4].copy_from_slice(&(len as u32).to_le_bytes());
    wbuf[4..4 + len].copy_from_slice(text.as_bytes());

    // Send request
    socket.write_all(&wbuf[..4 + len])?;

    // Prepare read buffer: header + max body + 1 byte
    let mut rbuf = [0u8; 4 + K_MAX_MSG + 1];

    // Read 4-byte header
    socket.read_exact(&mut rbuf[..4])?;
    let reply_len = u32::from_le_bytes(rbuf[..4].try_into().unwrap()) as usize;

    if reply_len > K_MAX_MSG {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "Reply too long"));
    }

    // Read reply body
    socket.read_exact(&mut rbuf[4..4 + reply_len])?;

    // Print reply
    println!(
        "Server says: {}",
        String::from_utf8_lossy(&rbuf[4..4 + reply_len])
    );

    Ok(())
}

fn hash_std(data: &[u8]) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    
    let mut hasher = DefaultHasher::new();
    data.hash(&mut hasher);
    hasher.finish()
}

fn main() -> std::io::Result<()> {

    // Optional: Demo tree before switching to client/server
    let mut root: Option<Rc<RefCell<Node<i32>>>> = None;

    tree_insert(&mut root, 5);
    tree_insert(&mut root, 2);
    tree_insert(&mut root, 8);

    if let Some(node) = tree_search(&root, &2) {
        println!("Found key: {}", node.borrow().key);
    } else {
        println!("Key not found");
    }

    let mut root: Option<Rc<RefCell<Node<i32>>>> = None;
    // ... insert some values ...   
    root = tree_delete(root, &5); // delete key = 5
    println!("Tree operations complete, root is: {:?}", root.is_some());


    let args: Vec<String> = env::args().collect();
    
    if args.len() > 1 && args[1] == "client" {
        run_client()
    } else {
        run_server()
    }
}