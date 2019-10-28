package onthego.database.core.index;

import static java.util.stream.Collectors.joining;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Stack;

import onthego.database.core.tablespace.manager.TablespaceManager;

public class BTreeIndex<T extends Comparable<? super T>> {
		
	static class Node<T> {
		boolean isLeaf;
		int n;
		long pos;
		
		T[] key;
		long[] recordPos;
		
		Node<T>[] child;
		long[] childPos;
		
		Node(int threshold, long pos) {
			this.isLeaf = false;
			this.n = 0;
			this.key = (T[])new Comparable[2*threshold - 1];
			this.recordPos = new long[2*threshold - 1];
			this.pos = pos;
			
			this.child = new Node[2*threshold];
			this.childPos = new long[2*threshold];
		}
		
		Node(int threshold, boolean isLeaf, long pos) {
			this(threshold, pos);
			this.isLeaf = isLeaf;
		}
		
//		Node<T> getChild(int index) {
//			if (child[index] == null) {
//				return (Node<T>)BTree.this.loadNode(childPos[index]);
//			}
//			
//			return child[index];
//		}
		
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
	
	private static final Node NULL_NODE = new Node(1, 0);
	
	private static final Pair NULL_PAIR = new Pair(NULL_NODE, 1);
	
	private final int threshold;
	
	private Comparator<T> comparator;
	
	private TablespaceManager tsManager;
	
	private Node<T> root;
	
	private int estimatedNodeSize;
	
	public BTreeIndex(int threshold, TablespaceManager tsManager) {
		this.threshold = threshold;
		this.comparator = Comparator.naturalOrder();
		this.tsManager = tsManager;
		this.estimatedNodeSize = estimateNodeSize();
		initialize();
	}
	
	public BTreeIndex(int threshold, Comparator<T> comparator, TablespaceManager tsManager) {
		this(threshold, tsManager);
		this.comparator = comparator;
	}
	
	private void initialize() {
		if (tsManager.getRootPos() == 0) {
			this.root = allocateNode(true);
		} else  {
			this.root = loadNode(tsManager.getRootPos());
		}
	}
	
//	boolean isLeaf;
//	int n;
//	T[] key;
//	long pos;
//	
//	Node<T>[] child;
//	long[] childPos;
	
	private byte[] generatePayload(Node<T> node) {
		try (ByteArrayOutputStream bout = new ByteArrayOutputStream();
			 ObjectOutputStream out = new ObjectOutputStream(bout)) {
			out.writeBoolean(node.isLeaf);
			out.writeInt(node.n);
			
			for (int i = 0; i < node.key.length; ++i) {
				try {
					out.writeObject(node.key[i]);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
			
			for (int i = 0; i < node.recordPos.length; ++i) {
				out.writeLong(node.recordPos[i]);
			}
			
			for (int i = 0; i < node.childPos.length; ++i) {
				out.writeLong(node.childPos[i]);
			}
			
			return bout.toByteArray();
		} catch(IOException ioe) {
			throw new RuntimeException(ioe);
		}
	}
	
	private int estimateNodeSize() {
		return generatePayload(new Node<T>(threshold, 0)).length;
	}
	
	//this is scheduled to be refactored with a newly defined exception
	private void saveNode(Node<T> node) {
		byte[] payload = generatePayload(node);
		try {
			tsManager.writeBlock(node.pos, payload);
		} catch (Exception e) {
			freeNode(node);
			this.estimatedNodeSize = payload.length;
			node.pos = tsManager.allocate(payload.length);
			tsManager.writeBlock(node.pos, payload);
		}
	}
	
	private Node<T> loadNode(long pos) {
		try (ByteArrayInputStream bin = new ByteArrayInputStream(tsManager.readBlock(pos));
			 ObjectInputStream in = new ObjectInputStream(bin)) {
			Node<T> node = new Node<T>(threshold, pos);
			node.isLeaf = in.readBoolean();
			node.n = in.readInt();
			
			for (int i = 0; i < node.key.length; ++i) {
				node.key[i] = (T)in.readObject();
			}
			
			for (int i = 0; i < node.recordPos.length; ++i) {
				node.recordPos[i] = in.readLong();
			}
			
			for (int i = 0; i < node.childPos.length; ++i) {
				node.childPos[i] = in.readLong();
			}
			
			return node;
		} catch(Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	private Node<T> allocateNode(boolean isLeaf) {
		long pos = tsManager.allocate(this.estimatedNodeSize);
		return new Node<T>(threshold, isLeaf, pos);
	}
	
	private void freeNode(Node<T> node) {
		tsManager.free(node.pos);
	}
	
	private Node<T> loadChild(Node<T> parent, int index) {
		if (parent.child[index] == null) {
			parent.child[index] = loadNode(parent.childPos[index]);
		}
		return parent.child[index];
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
//			return search(node.child[i], key);
			return search(loadChild(node, i), key);
		}
	}
	
	public boolean contains(T key) {
		return search(root, key) != NULL_PAIR ? true : false;
	}
	
	//split successor node(node.child[index]) 
	private void splitChild(Node<T> parent, int index) {
//		Node<T> successor = node.child[index];
		Node<T> successor = loadChild(parent, index);
//		Node<T> sibling = new Node<T>(threshold);
		Node<T> sibling = allocateNode(successor.isLeaf);
		
		// new sibling
//		sibling.isLeaf = successor.isLeaf;
		sibling.n = threshold - 1;
		
		for (int i = 0; i < threshold - 1; ++i) {
			sibling.key[i] = successor.key[threshold + i];
			sibling.recordPos[i] = successor.recordPos[threshold + i];
		}
		
		if (!sibling.isLeaf) {
			for (int i = 0; i < threshold; ++i) {
				sibling.childPos[i] = successor.childPos[threshold + i];
				sibling.child[i] = successor.child[threshold + i];
			}
		}
		
		//parent
		for (int i = parent.n - 1; i >= index; --i) {
			parent.key[i + 1] = parent.key[i];
			parent.recordPos[i + 1] = parent.recordPos[i];
		}
		
		for (int i = parent.n; i > index; --i) {
			parent.childPos[i + 1] = parent.childPos[i];
			parent.child[i + 1] = parent.child[i];
		}
		
		parent.n++;
		
		parent.childPos[index + 1] = sibling.pos;
		parent.child[index + 1] = sibling;
		
		parent.key[index] = successor.key[threshold - 1];
		parent.recordPos[index] = successor.recordPos[threshold - 1];
		
		successor.n = threshold - 1;
		
		saveNode(parent);
		saveNode(successor);
		saveNode(sibling);
	}
	
	private void insert(Node<T> node, T key, long recordPos) {
		if (node.isLeaf) {
			int index = node.n - 1;
			//while (index >= 0 && comparator.compare(key, node.key[index]) < 0) {	
			while (index >= 0 && comparator.compare(key, node.key[index]) <= 0) {
				node.key[index + 1] = node.key[index];
				node.recordPos[index + 1] = node.recordPos[index];
				--index;
			}
			
			node.key[index + 1] = key;
			node.recordPos[index + 1] = recordPos;
			node.n++;
			saveNode(node);
		} else {
			int index = node.n - 1;
			//while (index >= 0 && comparator.compare(key, node.key[index]) < 0) {	
			while (index >= 0 && comparator.compare(key, node.key[index]) <= 0) {
				--index;
			}
			++index;
			
			//Node<T> successor = node.child[index];
			Node<T> successor = loadChild(node, index);
			if (successor.n == 2*threshold - 1) {
				splitChild(node, index); 
				if (comparator.compare(key, node.key[index]) > 0) {
					successor = loadChild(node, index + 1);
					//successor = node.child[index + 1];
				}
			}
			
			insert(successor, key, recordPos);
		}
	}
	
	public void insert(T key, long recordPos) {
		//check if the input key has already been inserted into this index tree
		if (contains(key)) {
			throw new RuntimeException("duplicate key");
		}
		
		//in case that root node is full
		if (root.n == 2*threshold - 1) {
			//Node<T> newRoot = new Node<T>(threshold);
			Node<T> newRoot = allocateNode(false);
			//newRoot.n = 0;
			//newRoot.isLeaf = false;
			newRoot.child[0] = root;
			newRoot.childPos[0] = root.pos;
			saveNode(newRoot);
			
			root = newRoot;
			tsManager.saveRootPos(newRoot.pos);
			
			splitChild(root, 0);
		}
		
		insert(root, key, recordPos);
	}
	
	private Node<T> merge(Node<T> parent, int index, Node<T> successor, Node<T> sibling) {
		successor.key[successor.n] = parent.key[index];
		successor.recordPos[successor.n] = parent.recordPos[index];
		successor.n++;
		
		for (int j = 0; j < sibling.n; ++j) {
			successor.key[successor.n + j] = sibling.key[j];
			successor.recordPos[successor.n + j] = sibling.recordPos[j];
		}
		
		if (!sibling.isLeaf) {
			for (int j = 0; j <= sibling.n; ++j) {
				successor.childPos[successor.n + j] = sibling.childPos[j];
				successor.child[successor.n + j] = sibling.child[j];
			}
		}
		
		successor.n += sibling.n;
		
		for (int j = index + 1; j < parent.n; ++j) {
			parent.key[j - 1] = parent.key[j];
			parent.recordPos[j - 1] = parent.recordPos[j];
		}
		
		for (int j = index + 2; j <= parent.n; ++j) {
			parent.childPos[j - 1] = parent.childPos[j];
			parent.child[j - 1] = parent.child[j];
		}
		
		parent.n--;
		
		//in case that the parent node becomes an empty root node
		if (parent == root && parent.n == 0) {
			this.root = successor;
			tsManager.saveRootPos(successor.pos);
		} else {
			saveNode(parent);
		}
		
		saveNode(successor);
		freeNode(sibling);
		return successor;
	}
	
	private Pair<T,Long> findMaxKey(Node<T> node) {
		while (!node.isLeaf) {
			//node = node.child[node.n];
			node = loadChild(node, node.n);
		}
		return new Pair<>(node.key[node.n - 1], node.recordPos[node.n - 1]); 
	}
	
	private Pair<T,Long> findMinKey(Node<T> node) {
		while (!node.isLeaf) {
			//node = node.child[0];
			node = loadChild(node, 0);
		}
		return new Pair<>(node.key[0], node.recordPos[0]); 
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
					node.recordPos[i] = node.recordPos[i + 1];
					++i;
				}
				node.n--;
				saveNode(node);
				return true;
			} 
			return false;
		}
		
		// in case that the matched key is in an internal node
		if (i < node.n && comparator.compare(key, node.key[i]) == 0) {
//			Node<T> leftChild = node.child[i];
//			Node<T> rightChild = node.child[i + 1];
			Node<T> leftChild = loadChild(node, i);
			Node<T> rightChild = loadChild(node, i + 1);
			
			if (leftChild.n >= threshold) {
				Pair<T,Long> predKey = findMaxKey(leftChild);
				node.key[i] = predKey.first;
				node.recordPos[i] = predKey.second;
				saveNode(node);
				return delete(leftChild, predKey.first);
			} else if (rightChild.n >= threshold) {
				Pair<T,Long> succKey = findMinKey(rightChild);
				node.key[i] = succKey.first;
				node.recordPos[i] = succKey.second;
				saveNode(node);
				return delete(rightChild, succKey.first);
			} else {
				Node<T> successor = merge(node, i, leftChild, rightChild);			
				return delete(successor, key);
			}
		} else { //(i < node.n && comparator.compare(key, node.key[i]) < 0) || (i == node.n)
//			Node<T> successor = node.child[i];
			Node<T> successor = loadChild(node, i);

			if (successor.n < threshold) {
//				Node<T> rightSibling = (i < node.n) ? node.child[i + 1] : null;
//				Node<T> leftSibling = (i > 0) ? node.child[i - 1] : null;
				Node<T> rightSibling = (i < node.n) ? loadChild(node, i + 1) : null;
				Node<T> leftSibling = (i > 0) ? loadChild(node, i - 1) : null;
				
				if (rightSibling != null && rightSibling.n >= threshold) {
					//transplant : parent -> successor
					successor.key[successor.n] = node.key[i];
					successor.recordPos[successor.n] = node.recordPos[i];
					successor.n++;
					
					//transplant : sibling -> parent
					node.key[i] = rightSibling.key[0];
					node.recordPos[i] = rightSibling.recordPos[0];
					if (!rightSibling.isLeaf) {
						successor.childPos[successor.n] = rightSibling.childPos[0];
						successor.child[successor.n] = rightSibling.child[0];
					}
					
					//moving all elements one by one left-hand side
					for (int j = 0; j < rightSibling.n - 1; ++j) {
						rightSibling.key[j] = rightSibling.key[j + 1];
						rightSibling.recordPos[j] = rightSibling.recordPos[j + 1];
					}
					if (!rightSibling.isLeaf) {
						for (int j = 0; j < rightSibling.n; ++j) {
							rightSibling.childPos[j] = rightSibling.childPos[j + 1];
							rightSibling.child[j] = rightSibling.child[j + 1];
						}
					}
					
					rightSibling.n--;
					saveNode(node);
					saveNode(successor);
					saveNode(rightSibling);
				} else if (leftSibling != null && leftSibling.n >= threshold) {
					//move all elements one by one left-hand side
					for (int j = successor.n - 1; j >= 0; --j) {
						successor.key[j + 1] = successor.key[j];
						successor.recordPos[j + 1] = successor.recordPos[j];
					}
					if (!successor.isLeaf) {
						for (int j = successor.n; j >= 0; --j) {
							successor.childPos[j + 1] = successor.childPos[j];
							successor.child[j + 1] = successor.child[j];
						}
					}
					
					//transplant : parent -> successor
					successor.key[0] = node.key[i - 1];
					successor.recordPos[0] = node.recordPos[i - 1];
					successor.n++;
					
					//transplant : left sibling -> parent
					node.key[i - 1] = leftSibling.key[leftSibling.n - 1];
					node.recordPos[i - 1] = leftSibling.recordPos[leftSibling.n - 1];
					if (!leftSibling.isLeaf) {
						successor.childPos[0] = leftSibling.childPos[leftSibling.n];
						successor.child[0] = leftSibling.child[leftSibling.n];
					}
					
					leftSibling.n--;
					
					saveNode(node);
					saveNode(successor);
					saveNode(leftSibling);
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
//						queue.add(node.child[i]);
						queue.add(loadChild(node, i));
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
//				printSequentialOrder(node.child[index]);
				printSequentialOrder(loadChild(node, index));
				System.out.print(node.key[index] + " ");
				++index;
			}
//			printSequentialOrder(node.child[index]);
			printSequentialOrder(loadChild(node, index));
		}
	}
	
	public void printSequentialOrder() {
		printSequentialOrder(root);
		System.out.println();
	}
}
