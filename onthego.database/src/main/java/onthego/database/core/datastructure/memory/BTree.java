package onthego.database.core.datastructure.memory;

import static java.util.stream.Collectors.joining;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Stack;

public class BTree<T extends Comparable<? super T>> {
		
	static class Node<T> {
		boolean isLeaf;
		int n;
		T[] key;
		Node<T>[] child;
		
		Node(int threshold) {
			this.isLeaf = false;
			this.n = 0;
			this.key = (T[])new Comparable[2*threshold - 1];
			this.child = new Node[2*threshold];
		}
		
		Node(int threshold, boolean isLeaf) {
			this(threshold);
			this.isLeaf = isLeaf;
		}
		
		@Override
		public String toString() {
			return Arrays.stream(key, 0, n).map(String::valueOf)
									 	   .collect(joining(",", "[", "]"));
		}
	}
	
	static class Pair<S,V>{
		S first;
		V second;
		
		Pair(S first, V second) {
			this.first = first;
			this.second = second;
		}
	}
	
	private class BTreeIterator implements Iterator<T> {
		
		private static final int INDEX_TYPE_CHILD = 0;
		
		private static final int INDEX_TYPE_KEY = 1;
		
		private Map<Node<T>,Pair<Integer,Integer>> map;
		
		private Stack<Node<T>> stack;
		
		public BTreeIterator() {
			if (root != null && root.n > 0) {
				this.map = new HashMap<>();
				this.stack = new Stack<>();
				stack.push(root);
				map.put(root, new Pair<>(INDEX_TYPE_CHILD, 0));
			}
		}

		@Override
		public boolean hasNext() {
			return (stack != null && !stack.isEmpty());
		}

		@Override
		public T next() {
			if (!hasNext()) {
				return null;
			}
			
			Node<T> currentNode = stack.peek();
			int indexType = map.get(currentNode).first;
			int currentIndex = map.get(currentNode).second;
			
			if (currentNode.isLeaf && currentIndex < currentNode.n) {
				if (currentIndex == currentNode.n - 1) {
					stack.pop();
					map.remove(currentNode);
				} else {
					map.put(currentNode, new Pair<>(INDEX_TYPE_KEY,currentIndex + 1));
				}
			} else if (indexType == INDEX_TYPE_KEY && currentIndex < currentNode.n) {
				map.put(currentNode, new Pair<>(INDEX_TYPE_CHILD,currentIndex + 1));
			} else if (indexType == INDEX_TYPE_CHILD && currentIndex <= currentNode.n) {
				while (!currentNode.isLeaf) {
					if (currentIndex < currentNode.n) {
						//A key of current node is scheduled to be accessed next time
						map.put(currentNode, new Pair<>(INDEX_TYPE_KEY, currentIndex));
					} else {
						//the scan of current node is completed
						stack.pop();
						map.remove(currentNode);
					}
					
					//a newly found internal node
					currentNode = currentNode.child[currentIndex];
					currentIndex = 0;
					stack.push(currentNode);
					//map.put(currentNode, new Pair<>(INDEX_TYPE_KEY, 0));
				}
					
				//in case of reaching to a leaf node
				map.put(currentNode, new Pair<>(INDEX_TYPE_KEY, 1));
				//currentIndex = 0;
			}
			
			return currentNode.key[currentIndex];
		}
	}
	
	private static final Node NULL_NODE = new Node(1);
	
	private static final Pair NULL_PAIR = new Pair(NULL_NODE, 1);
	
	private final int threshold;
	
	private Node<T> root;
	
	private Comparator<T> comparator;
	
	public BTree(int threshold) {
		this.threshold = threshold;
		this.root = new Node<T>(threshold, true);
		this.comparator = Comparator.naturalOrder();
	}
	
	public BTree(int threshold, Comparator<T> comparator) {
		this(threshold);
		this.comparator = comparator;
	}
	
	private Pair<Node<T>,Integer> search(Node<T> node, T key) {
		if (node == null) {
			return NULL_PAIR;
		}
		
		int i = 0;
		while (i < node.n && comparator.compare(key, node.key[i]) > 0) {
			++i;
		}
		
		if (i < node.n && comparator.compare(key, node.key[i]) == 0) { //equal to the key
			return new Pair<Node<T>,Integer>(node, i);
		} else if (node.isLeaf) { // could not find the key in the leaf node, there is no the key in BTree
			return NULL_PAIR;
		} else { //less than the key
			return search(node.child[i], key);
		}
	}
	
	public boolean contains(T key) {
		return search(root, key) != NULL_PAIR ? true : false;
	}
	
	//split successor node(node.child[index]) 
	private void splitChild(Node<T> node, int index) {
		Node<T> successor = node.child[index];
		Node<T> sibling = new Node<T>(threshold);
		
		// new sibling
		sibling.isLeaf = successor.isLeaf;
		sibling.n = threshold - 1;
		
		for (int i = 0; i < threshold - 1; ++i) {
			sibling.key[i] = successor.key[threshold + i];
		}
		
		if (!sibling.isLeaf) {
			for (int i = 0; i < threshold; ++i) {
				sibling.child[i] = successor.child[threshold + i];
			}
		}
		
		//parent
		for (int i = node.n - 1; i >= index; --i) {
			node.key[i + 1] = node.key[i];
		}
		
		for (int i = node.n; i > index; --i) {
			node.child[i + 1] = node.child[i];
		}
		
		node.n++;
		node.child[index + 1] = sibling;
		node.key[index] = successor.key[threshold - 1];
		
		successor.n = threshold - 1;
	}
	
	private void insert(Node<T> node, T key) {
		if (node.isLeaf) {
			int index = node.n - 1;
			//while (index >= 0 && comparator.compare(key, node.key[index]) < 0) {	
			while (index >= 0 && comparator.compare(key, node.key[index]) <= 0) {
				node.key[index + 1] = node.key[index];
				--index;
			}
			node.key[index + 1] = key;
			node.n++;
		} else {
			int index = node.n - 1;
			//while (index >= 0 && comparator.compare(key, node.key[index]) < 0) {	
			while (index >= 0 && comparator.compare(key, node.key[index]) <= 0) {
				--index;
			}
			++index;
			
			Node<T> successor = node.child[index];
			if (successor.n == 2*threshold - 1) {
				splitChild(node, index); 
				if (comparator.compare(key, node.key[index]) > 0) {
					successor = node.child[index + 1];
				}
			}
			
			insert(successor, key);
		}
	}
	
	public void insert(T key) {
		//in case that root node is full
		if (root.n == 2*threshold - 1) {
			Node<T> newRoot = new Node<T>(threshold);
			//newRoot.n = 0;
			//newRoot.isLeaf = false;
			newRoot.child[0] = root;
			root = newRoot;
			splitChild(root, 0);
		}
		
		insert(root, key);
	}
	
	private Node<T> merge(Node<T> parent, int index, Node<T> successor, Node<T> sibling) {
		successor.key[successor.n++] = parent.key[index];
		
		for (int j = 0; j < sibling.n; ++j) {
			successor.key[successor.n + j] = sibling.key[j];
		}
		
		if (!sibling.isLeaf) {
			for (int j = 0; j <= sibling.n; ++j) {
				successor.child[successor.n + j] = sibling.child[j];
			}
		}
		
		successor.n += sibling.n;
		
		for (int j = index + 1; j < parent.n; ++j) {
			parent.key[j - 1] = parent.key[j];
		}
		
		for (int j = index + 2; j <= parent.n; ++j) {
			parent.child[j - 1] = parent.child[j];
		}
		
		parent.n--;
		
		//in case that the parent node becomes an empty root node
		if (parent == root && parent.n == 0) {
			this.root = successor;
		}
		
		return successor;
	}
	
	private T findMaxKey(Node<T> node) {
		while (!node.isLeaf) {
			node = node.child[node.n];
		}
		return node.key[node.n - 1]; 
	}
	
	private T findMinKey(Node<T> node) {
		while (!node.isLeaf) {
			node = node.child[0];
		}
		return node.key[0]; 
	}
	
	private boolean delete(Node<T> node, T key) {
		int i = 0;
		while (i < node.n && comparator.compare(key, node.key[i]) > 0) {
			++i;
		}
				
		// in case of reaching to leaf node
		if (node.isLeaf) {
			if (i < node.n && comparator.compare(key, node.key[i]) == 0) {
				while (i < node.n - 1) {
					node.key[i] = node.key[i + 1];
					++i;
				}
				node.n--;
				return true;
			} 
			return false;
		}
		
		// in case that the matched key is in an internal node
		if (i < node.n && comparator.compare(key, node.key[i]) == 0) {
			Node<T> leftChild = node.child[i];
			Node<T> rightChild = node.child[i + 1];
			
			if (leftChild.n >= threshold) {
				T predKey = findMaxKey(leftChild);
				node.key[i] = predKey;
				return delete(leftChild, predKey);
			} else if (rightChild.n >= threshold) {
				T succKey = findMinKey(rightChild);
				node.key[i] = succKey;
				return delete(rightChild, succKey);
			} else {
				Node<T> successor = merge(node, i, leftChild, rightChild);			
				return delete(successor, key);
			}
		} else { //(i < node.n && comparator.compare(key, node.key[i]) < 0) || (i == node.n)
			Node<T> successor = node.child[i];

			if (successor.n < threshold) {
				Node<T> rightSibling = (i < node.n) ? node.child[i + 1] : null;
				Node<T> leftSibling = (i > 0) ? node.child[i - 1] : null;
				
				if (rightSibling != null && rightSibling.n >= threshold) {
					//transplant : parent -> successor
					successor.key[successor.n] = node.key[i];
					successor.n++;
					
					//transplant : sibling -> parent
					node.key[i] = rightSibling.key[0];
					if (!rightSibling.isLeaf) {
						successor.child[successor.n] = rightSibling.child[0];
					}
					
					//moving all elements one by one left-hand side
					for (int j = 0; j < rightSibling.n - 1; ++j) {
						rightSibling.key[j] = rightSibling.key[j + 1];
					}
					if (!rightSibling.isLeaf) {
						for (int j = 0; j < rightSibling.n; ++j) {
							rightSibling.child[j] = rightSibling.child[j + 1];
						}
					}
					rightSibling.n--;
				} else if (leftSibling != null && leftSibling.n >= threshold) {
					//move all elements one by one left-hand side
					for (int j = successor.n - 1; j >= 0; --j) {
						successor.key[j + 1] = successor.key[j];
					}
					if (!successor.isLeaf) {
						for (int j = successor.n; j >= 0; --j) {
							successor.child[j + 1] = successor.child[j];
						}
					}
					
					//transplant : parent -> successor
					successor.key[0] = node.key[i - 1];
					successor.n++;
					
					//transplant : left sibling -> parent
					node.key[i - 1] = leftSibling.key[leftSibling.n - 1];
					if (!leftSibling.isLeaf) {
						successor.child[0] = leftSibling.child[leftSibling.n];
					}
					leftSibling.n--;
				} else {
					if (rightSibling != null) {
						successor = merge(node, i, successor, rightSibling);			
					} else if (leftSibling != null) {
						successor = merge(node, i - 1, leftSibling, successor);
					}
				}
			}
			
			return delete(successor, key);
		} 
	}

	public boolean delete(T key) {
		return delete(root, key);
	}
	
	public Iterator<T> iterator() {
		return new BTreeIterator();
	}
	
	public void printLevelOrder() {
		if (root == null) {
			return;
		}
		
		Queue<Node<T>> queue = new LinkedList<>();
		queue.add(root);
		
		while(!queue.isEmpty()) {
			int counter = queue.size();
			while (counter-- > 0) {
				Node<T> node = queue.poll();
				System.out.print(node);
				
				if (!node.isLeaf) {
					for (int i = 0; i <= node.n; ++i) {
						queue.add(node.child[i]);
					}
				}
			}
			System.out.println();
		}
	}
	
	private void printSequentialOrder(Node<T> node) {
		if (node.isLeaf) {
			Arrays.stream(node.key, 0, node.n).forEach(key -> System.out.print(key + " "));
		} else {
			int index = 0;
			while (index < node.n) {
				printSequentialOrder(node.child[index]);
				System.out.print(node.key[index] + " ");
				++index;
			}
			printSequentialOrder(node.child[index]);
		}
	}
	
	public void printSequentialOrder() {
		printSequentialOrder(root);
		System.out.println();
	}
}
