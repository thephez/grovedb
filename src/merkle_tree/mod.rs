extern crate blake3;

use std::rc::Rc;
use std::cell::RefCell;

type Link<T> = Rc<RefCell<T>>;

#[derive(Debug, Default)]
pub struct LeafNode {
  key: Vec<u8>,
  value_hash: [u8; 32],
  value: Option<Data>,
}

#[derive(Debug, Default)]
pub struct CommitedInnerNode {
  left: Option<Link<TreeNode>>,
  right: Option<Link<TreeNode>>
}

#[derive(Debug, Default)]
pub struct UncommitedInnerNode {
  old: Link<TreeNode>,
  new: Link<TreeNode>,
}

impl UncommitedInnerNode {
  fn get_left(&self) -> Option<Link<TreeNode>> {
    self.new.borrow().get_left().as_ref().map(|rc| rc.clone())
  }

  fn set_left(&mut self, node: TreeNode) {
    self.new.borrow_mut().set_left(node);
  }

  fn get_right(&self) -> Option<Link<TreeNode>> {
    self.new.borrow().get_right().as_ref().map(|rc| rc.clone())
  }

  fn set_right(&mut self, node: TreeNode) {
    self.new.borrow_mut().set_right(node);
  }
}

#[derive(Debug, Default)]
pub enum InnerNode {
  CommitedInnerNode(CommitedInnerNode),
  UncommitedInnerNode(UncommitedInnerNode),
  #[default]
  None,
}

impl InnerNode {
  fn get_left(&self) -> Option<Link<TreeNode>> {
    match self {
      InnerNode::CommitedInnerNode(inner) => inner.left.as_ref().map(|rc| rc.clone()),
      InnerNode::UncommitedInnerNode(inner) => inner.get_left(),
      InnerNode::None => None
    }
  }

  fn set_left(&mut self, node: TreeNode) {
    match self {
      InnerNode::CommitedInnerNode(inner) => inner.left = Some(Rc::new(RefCell::new(node))),
      InnerNode::UncommitedInnerNode(inner) => inner.set_left(node),
      InnerNode::None => ()
    }
  }

  fn get_right(&self) -> Option<Link<TreeNode>> {
    match self {
      InnerNode::CommitedInnerNode(inner) => inner.right.as_ref().map(|rc| rc.clone()),
      InnerNode::UncommitedInnerNode(inner) => inner.get_right(),
      InnerNode::None => None
    }
  }

  fn set_right(&mut self, node: TreeNode) {
    match self {
      InnerNode::CommitedInnerNode(inner) => inner.right = Some(Rc::new(RefCell::new(node))),
      InnerNode::UncommitedInnerNode(inner) => inner.set_right(node),
      InnerNode::None => ()
    }
  }
}

#[derive(Debug, Default)]
pub enum TreeNode {
  LeafNode(LeafNode),
  InnerNode(InnerNode),
  #[default]
  None,
}

#[derive(Debug)]
pub enum Data {
  ValueData(Vec<u8>),
  TreeData(Box<MerkleTree>),
  SecondaryIndexData([u8; 32])
}

impl Data {
  fn hash(&self) -> [u8; 32] {
    match self {
      Data::ValueData(value_data) => {
        let mut result = [0; 32];
        result.clone_from_slice(blake3::hash(&value_data).as_bytes());
        result
      },
      Data::TreeData(tree) => tree.root_node.borrow().hash(),
      Data::SecondaryIndexData(index_data) => {
        let mut result = [0; 32];
        result.clone_from_slice(index_data);
        result
      },
    }
  }
}

impl TreeNode {
  fn hash(&self) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();

    match self {
      TreeNode::LeafNode(LeafNode { key, value:_, value_hash }) => {
        hasher.update(&key);
        hasher.update(value_hash);
      },
      TreeNode::InnerNode(inner) => {
        match inner.get_right() {
          Some(right) => {
            hasher.update(&inner.get_left().unwrap().borrow().hash());
            hasher.update(&right.borrow().hash());
          },
          None => {
            hasher.update(&inner.get_left().unwrap().borrow().hash());
          }
        }
      },
      TreeNode::None => ()
    };

    let mut result = [0; 32];
    result.clone_from_slice(hasher.finalize().as_bytes());
    result
  }

  fn get_left(&self) -> Option<Link<TreeNode>> {
    match self {
      TreeNode::LeafNode(_) => None,
      TreeNode::InnerNode(inner) => inner.get_left(),
      TreeNode::None => None,
    }
  }

  fn set_left(&mut self, node: TreeNode) {
    match self {
      TreeNode::LeafNode(_) => (),
      TreeNode::InnerNode(inner) => inner.set_left(node),
      TreeNode::None => (),
    }
  }

  fn get_right(&self) -> Option<Link<TreeNode>> {
    match self {
      TreeNode::LeafNode(_) => None,
      TreeNode::InnerNode(inner) => inner.get_right(),
      TreeNode::None => None,
    }
  }

  fn set_right(&mut self, node: TreeNode) {
    match self {
      TreeNode::LeafNode(_) => (),
      TreeNode::InnerNode(inner) => inner.set_right(node),
      TreeNode::None => (),
    }
  }

  fn get_value(&self) -> Option<&Data> {
    match self {
      TreeNode::LeafNode(LeafNode { key: _, value_hash: _, value }) => value.as_ref(),
      TreeNode::InnerNode { .. } => None,
      TreeNode::None => None,
    }
  }

  fn is_leaf_node(&self) -> bool {
    match self {
      TreeNode::LeafNode(_) => true,
      TreeNode::InnerNode(_) => false,
      TreeNode::None => false,
    }
  }

  fn get_height(&self) -> u64 {
    if self.get_left().is_none() {
      return 0;
    }

    fn find_bottom(node_option: Option<Link<TreeNode>>, counter: u64) -> (Option<Link<TreeNode>>, u64) {
      if node_option.is_none() {
        return (None, counter)
      }

      let node_rc = node_option.as_ref().map(|rc| rc.clone()).unwrap();

      let node = node_rc.borrow();

      let clone = node_option.as_ref().map(|rc| rc.clone());

      if node.is_leaf_node() {
        return (clone, counter);
      }

      find_bottom(node.get_left(), counter + 1)
    }

    find_bottom(self.get_left(), 1).1
  }
}

#[derive(Debug)]
pub struct MerkleTree {
  root_node: Link<TreeNode>,
}

impl MerkleTree {
  pub fn new(mut key_values: Vec<(Vec<u8>, Data)>) -> Self {
    if key_values.is_empty() {
      panic!("at least one key-value item should be submitted")
    }

    let leaf_nodes: Vec<TreeNode> = key_values.drain(0..).map(|(key, value)| {
      TreeNode::LeafNode(
        LeafNode {
          key: key.to_vec(),
          value_hash: value.hash(),
          value: Some(value),
        }
      )
    }).collect();

    let root_node = MerkleTree::build_root_node(leaf_nodes);

    MerkleTree {
      root_node: Rc::new(RefCell::new(root_node)),
    }
  }

  fn build_root_node(mut nodes: Vec<TreeNode>) -> TreeNode {
    nodes = nodes.into_iter().rev().collect();

    let mut result_nodes: Vec<TreeNode> = vec!();
    while nodes.len() > 0 {
      let left_node: TreeNode = nodes.pop().unwrap();

      let node = TreeNode::InnerNode(
        InnerNode::CommitedInnerNode(CommitedInnerNode {
          left: Some(Rc::new(RefCell::new(left_node))),
          right: nodes.pop().map(|leaf_node| Rc::new(RefCell::new(leaf_node))),
        })
      );

      result_nodes.push(node);
    }

    if result_nodes.len() == 1 {
      result_nodes.pop().unwrap()
    } else {
      MerkleTree::build_root_node(result_nodes)
    }
  }

  fn find_incomplete(&self) -> Option<Link<TreeNode>> {
    if self.root_node.borrow().get_right().is_none() {
      return Some(self.root_node.clone());
    }

    fn find(node_option: Option<Link<TreeNode>>) -> Option<Link<TreeNode>> {
      if node_option.is_none() {
        return None;
      }

      let node_rc = node_option.as_ref().map(|rc| rc.clone()).unwrap();

      let node = node_rc.borrow();

      let clone = node_option.as_ref().map(|rc| rc.clone());

      if node.is_leaf_node() {
        return None;
      }

      if node.get_right().is_none() {
        return clone;
      }

      find(node.get_right())
    }

    find(self.root_node.borrow().get_right())
  }

  pub fn get_height(&self) -> u64 {
    fn find_bottom(node_option: Option<Link<TreeNode>>, counter: u64) -> (Option<Link<TreeNode>>, u64) {
      if node_option.is_none() {
        return (None, counter)
      }

      let rc = node_option.as_ref().map(|rc| rc.clone()).unwrap();

      let node = rc.borrow();

      if node.is_leaf_node() {
        return (node_option, counter);
      }

      find_bottom(node.get_left(), counter + 1)
    }

    find_bottom(Some(self.root_node.clone()), 0).1
  }

  pub fn insert(&mut self, key: Vec<u8>, value: Data) -> Result<(), &'static str> {
    let result = self.find_incomplete();

    match result {
      Some(node) => match &mut *node.borrow_mut() {
        TreeNode::InnerNode(inner) => {
          let new_node = TreeNode::LeafNode(
            LeafNode {
              key,
              value_hash: [0; 32],
              value: Some(value),
            }
          );

          inner.set_right(new_node);

          Ok(())
        },
        _ => Ok(())
      }
      None => {
        let tree_height = self.get_height();

        let leaf_node = TreeNode::LeafNode(LeafNode {
          key,
          value_hash: value.hash(),
          value: Some(value),
        });

        let old_root = std::mem::take(&mut self.root_node);

        fn construct_new_chain(inner_node: TreeNode, counter: u64) -> TreeNode {
          if counter == 0 {
            return inner_node;
          }

          let top = TreeNode::InnerNode(InnerNode::CommitedInnerNode(CommitedInnerNode {
            left: Some(Rc::new(RefCell::new(inner_node))),
            right: None,
          }));

          construct_new_chain(top, counter - 1)
        }

        let new_right = construct_new_chain(
          TreeNode::InnerNode(InnerNode::CommitedInnerNode(CommitedInnerNode {
            left: Some(Rc::new(RefCell::new(leaf_node))),
            right: None
          })),
          tree_height
        );

        self.root_node = Rc::new(RefCell::new(TreeNode::InnerNode(InnerNode::CommitedInnerNode(CommitedInnerNode {
          left: Some(old_root),
          right: Some(Rc::new(RefCell::new(new_right))),
        }))));

        Ok(())
      },
    }
  }

  pub fn insert_tx(self, key: Vec<u8>, value: Data) -> MerkleTree {
    let result = self.find_incomplete();

    match result {
      Some(node) => {
        let tree_height = self.get_height();

        let old_root = self.root_node;

        fn construct_new_chain(inner_node: TreeNode, counter: u64) -> TreeNode {
          if counter == 0 {
            return inner_node;
          }

          let top = TreeNode::InnerNode(InnerNode::CommitedInnerNode(CommitedInnerNode {
            left: Some(Rc::new(RefCell::new(inner_node))),
            right: None,
          }));

          construct_new_chain(top, counter - 1)
        }

        let leaf_node = TreeNode::LeafNode(LeafNode {
          key,
          value_hash: value.hash(),
          value: Some(value),
        });

        let old_left_leaf = node.borrow().get_left();

        let last_shadow_inner_node = TreeNode::InnerNode(InnerNode::CommitedInnerNode(CommitedInnerNode {
          left: old_left_leaf.clone(),
          right: Some(Rc::new(RefCell::new(leaf_node))),
        }));

        let last_shadow_node = TreeNode::InnerNode(InnerNode::UncommitedInnerNode(UncommitedInnerNode {
          old: node.clone(),
          new: Rc::new(RefCell::new(last_shadow_inner_node)),
        }));

        let new_right = construct_new_chain(
          last_shadow_node,
          tree_height
        );

        let new_root = TreeNode::InnerNode(InnerNode::CommitedInnerNode(CommitedInnerNode {
          left: Some(old_root.clone()),
          right: Some(Rc::new(RefCell::new(new_right))),
        }));

        let root_node = TreeNode::InnerNode(InnerNode::UncommitedInnerNode(UncommitedInnerNode {
          old: old_root.clone(),
          new: Rc::new(RefCell::new(new_root)),
        }));

        MerkleTree {
          root_node: Rc::new(RefCell::new(root_node)),
        }
      },
      None => {
        let tree_height = self.get_height();

        let old_root = self.root_node;

        fn construct_new_chain(inner_node: TreeNode, counter: u64) -> TreeNode {
          if counter == 0 {
            return inner_node;
          }

          let top = TreeNode::InnerNode(InnerNode::CommitedInnerNode(CommitedInnerNode {
            left: Some(Rc::new(RefCell::new(inner_node))),
            right: None,
          }));

          construct_new_chain(top, counter - 1)
        }

        let leaf_node = TreeNode::LeafNode(LeafNode {
          key,
          value_hash: value.hash(),
          value: Some(value),
        });

        let new_right = construct_new_chain(
          TreeNode::InnerNode(InnerNode::CommitedInnerNode(CommitedInnerNode {
            left: Some(Rc::new(RefCell::new(leaf_node))),
            right: None
          })),
          tree_height
        );

        let new_root = TreeNode::InnerNode(InnerNode::CommitedInnerNode(CommitedInnerNode {
          left: Some(old_root.clone()),
          right: Some(Rc::new(RefCell::new(new_right))),
        }));

        let root_node = TreeNode::InnerNode(InnerNode::UncommitedInnerNode(UncommitedInnerNode {
          old: old_root.clone(),
          new: Rc::new(RefCell::new(new_root)),
        }));

        MerkleTree {
          root_node: Rc::new(RefCell::new(root_node)),
        }
      }
    }
  }

  pub fn rollback(self) -> MerkleTree {
    match &*self.root_node.clone().borrow() {
      TreeNode::InnerNode(inner) => match inner {
        InnerNode::UncommitedInnerNode(uncommited) => {
          MerkleTree {
            root_node: uncommited.old.clone(),
          }
        },
        InnerNode::CommitedInnerNode(_) => self,
        InnerNode::None => self,
      },
      TreeNode::LeafNode(_) => self,
      TreeNode::None => self,
    }
  }

  pub fn commit(self) -> MerkleTree {
    // TODO: make commit
    self
  }
}

#[test]
fn t1() {
  use super::hex_slice::hex_slice::HexDisplayExt;

  let imbalanced_tree = MerkleTree::new(vec!(
    (vec!(1, 2, 3), Data::ValueData(vec!(4, 5, 6))),
    (vec!(10, 20, 30), Data::ValueData(vec!(40, 50, 60))),
    (vec!(100, 200, 201), Data::SecondaryIndexData([1; 32])),
  ));

  // // checking we have nothing to the right
  // assert_eq!(imbalanced_tree.root_node.borrow().get_right().unwrap().borrow().get_right().is_none(), true);

  // imbalanced_tree.insert(vec!(0, 0, 0), Data::ValueData(vec!(0, 0, 0))).unwrap();

  // assert_eq!(imbalanced_tree.root_node.borrow().get_right().unwrap().borrow().get_right().is_none(), false);

  // let node = imbalanced_tree.root_node.borrow().get_right().unwrap().borrow().get_right().unwrap();

  // assert!(matches!(node.borrow().get_value().unwrap(), Data::ValueData(_)));

  println!("hash: {}", imbalanced_tree.root_node.borrow().hash().as_ref().hex_slice());

  let uncommited = imbalanced_tree.insert_tx(vec!(0, 9, 0), Data::ValueData(vec!(0, 9, 0)));

  println!("hash: {}", uncommited.root_node.borrow().hash().as_ref().hex_slice());

  let rolled_back = uncommited.rollback();

  println!("hash: {}", rolled_back.root_node.borrow().hash().as_ref().hex_slice());
}