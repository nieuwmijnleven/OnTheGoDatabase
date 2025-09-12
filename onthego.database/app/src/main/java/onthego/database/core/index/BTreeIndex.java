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

import onthego.database.core.exception.InsufficientPayloadSpaceException;
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
	
	class BTreeIterator implements Iterator<T> {
		
		static final int INDEX_TYPE_CHILD = 0;
		
		static final int INDEX_TYPE_KEY = 1;
		
		Map<Node<T>,Pair<Integer,Integer>> map;
		
		Stack<Node<T>> stack;
		
		BTreeIterator() {
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
			
			
			if (currentNode.isLeaf) {
				if (currentIndex >= currentNode.n - 1) {
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
					//currentNode = currentNode.child[currentIndex];
					currentNode = loadChild(currentNode, currentIndex);
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
	
	private final int threshold;
	
	private Comparator<T> comparator;
	
	private final TablespaceManager tsManager;
	
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
			tsManager.saveRootPos(root.pos);
			saveNode(root);
		} else  {
			this.root = loadNode(tsManager.getRootPos());
		}
	}
	
	private byte[] generatePayload(Node<T> node) {
		try (ByteArrayOutputStream bout = new ByteArrayOutputStream();
			 ObjectOutputStream out = new ObjectOutputStream(bout)) {
			out.writeBoolean(node.isLeaf);
			out.writeInt(node.n);
			
			for (int i = 0; i < node.key.length; ++i) {
				try {
					out.writeObject(node.key[i]);
				} catch (Exception e) {
					throw new BTreeIndexException(e);
				}
			}
			
			for (int i = 0; i < node.recordPos.length; ++i) {
				out.writeLong(node.recordPos[i]);
			}
			
			for (int i = 0; i < node.childPos.length; ++i) {
				out.writeLong(node.childPos[i]);
			}
			
			out.flush();
			return bout.toByteArray();
		} catch(IOException ioe) {
			throw new BTreeIndexException(ioe);
		}
	}
	
	private int estimateNodeSize() {
		return generatePayload(new Node<T>(threshold, 0)).length;
	}
	
	private void saveNode(Node<T> node) {
		byte[] payload = generatePayload(node);
		try {
			tsManager.writeBlock(node.pos, payload);
		} catch (InsufficientPayloadSpaceException e) {
			retrySaveWithAdjustedSize(node, payload);
		}
	}

	private void retrySaveWithAdjustedSize(Node<T> node, byte[] payload) {
		try {
			freeNode(node);
			this.estimatedNodeSize = payload.length;
			node.pos = tsManager.allocate(payload.length);
			if (node == root) {
				tsManager.saveRootPos(node.pos);
			}
			tsManager.writeBlock(node.pos, payload);
		} catch (InsufficientPayloadSpaceException e) {
			throw new BTreeIndexException("it's impossible to save a payload into a tablespace");
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
            e.printStackTrace();
			throw new BTreeIndexException(e);
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
	
	private long search(Node<T> node, T key) {
		if (node == null) {
			return -1;
		}
		
		int i = 0;
		while (i < node.n && comparator.compare(key, node.key[i]) > 0) {
			++i;
		}
		
		if (i < node.n && comparator.compare(key, node.key[i]) == 0) { //equal to the key
			return node.recordPos[i];
		} else if (node.isLeaf) { // could not find the key in the leaf node, there is no the key in BTree
			return -1;
		} else { //less than the key
			return search(loadChild(node, i), key);
		}
	}
	
	public boolean contains(T key) {
		return search(root, key) != -1;
	}
	
	private void assignKeyValue(Node<T> node, int index, T key, long recordPos) {
		node.key[index] = key;
		node.recordPos[index] = recordPos;
	}
	
	private void assignChildValue(Node<T> node, int index, Node<T> child, long childPos) {
		node.child[index] = child;
		node.childPos[index] = childPos;
	}
	
	private void assignKey(Node<T> dest, int destIndex, Node<T> src, int srcIndex) {
		dest.key[destIndex] = src.key[srcIndex];
		dest.recordPos[destIndex] = src.recordPos[srcIndex];
	}
	
	private void assignChild(Node<T> dest, int destIndex, Node<T> src, int srcIndex) {
		dest.child[destIndex] = src.child[srcIndex];
		dest.childPos[destIndex] = src.childPos[srcIndex];
	}
	
	private void transplantKey(Node<T> dest, int destIndex, Node<T> src, int srcIndex, int count) {
		for (int index = 0; index < count; ++index) {
			assignKey(dest, destIndex + index, src, srcIndex + index);
		}
	}
	
	private void transplantChild(Node<T> dest, int destIndex, Node<T> src, int srcIndex, int count) {
		for (int index = 0; index < count; ++index) {
			assignChild(dest, destIndex + index, src, srcIndex + index);
		}
	}
	
	private void moveBackKey(Node<T> node, int from) {
		for (int index = node.n - 1; index >= from; --index) {
			assignKey(node, index + 1, node, index);
		}
	}
	
	private void moveBackChild(Node<T> node, int from) {
		for (int index = node.n; index >= from; --index) {
			assignChild(node, index + 1, node, index);
		}
	}
	
	private void moveForwardKey(Node<T> node, int from) {
		for (int index = from + 1; index < node.n; ++index) {
			assignKey(node, index - 1, node, index);
		}
	}
	
	private void moveForwardChild(Node<T> node, int from) {
		for (int index = from + 1; index <= node.n; ++index) {
			assignChild(node, index - 1, node, index);
		}
	}
	
	//split successor node(node.child[index]) 
	private void splitChild(Node<T> parent, int index) {
		Node<T> successor = loadChild(parent, index);
		Node<T> sibling = allocateNode(successor.isLeaf);
		
		sibling.n = threshold - 1;
		transplantKey(sibling, 0, successor, threshold, threshold - 1);
		if (!sibling.isLeaf) {
			transplantChild(sibling, 0, successor, threshold, threshold);
		}
		
		moveBackKey(parent, index);
		moveBackChild(parent, index + 1);
		parent.n++;
		
		assignChildValue(parent, index + 1, sibling, sibling.pos);
		assignKey(parent, index, successor, threshold - 1);
		successor.n = threshold - 1;
		
		saveNode(parent);
		saveNode(successor);
		saveNode(sibling);
	}

	
	
	private void insert(Node<T> node, T key, long recordPos) {
		if (node.isLeaf) {
			int index = node.n - 1;
			while (index >= 0 && comparator.compare(key, node.key[index]) <= 0) {
				assignKey(node, index + 1, node, index);
				--index;
			}
			
			assignKeyValue(node, index + 1, key, recordPos);
			node.n++;
			saveNode(node);
		} else {
			int index = node.n - 1;
			while (index >= 0 && comparator.compare(key, node.key[index]) <= 0) {
				--index;
			}
			++index;
			
			Node<T> successor = loadChild(node, index);
			if (successor.n == 2*threshold - 1) {
				splitChild(node, index); 
				if (comparator.compare(key, node.key[index]) > 0) {
					successor = loadChild(node, index + 1);
				}
			}
			
			insert(successor, key, recordPos);
		}
	}
	
	public void insert(T key, long recordPos) {
		//check if the input key has already been inserted into this index tree
		if (contains(key)) {
			throw new BTreeIndexException("duplicate key");
		}
		
		//in case that root node is full
		if (root.n == 2*threshold - 1) {
			Node<T> newRoot = allocateNode(false);
			assignChildValue(newRoot, 0, root, root.pos);
			saveNode(newRoot);
			
			root = newRoot;
			tsManager.saveRootPos(newRoot.pos);
			
			splitChild(root, 0);
		}
		
		insert(root, key, recordPos);
	}
	
	private Node<T> merge(Node<T> parent, int index, Node<T> successor, Node<T> sibling) {
		assignKey(successor, successor.n, parent, index);
		successor.n++;
		
		transplantKey(successor, successor.n, sibling, 0, sibling.n);
		if (!sibling.isLeaf) {
			transplantChild(successor, successor.n, sibling, 0, sibling.n + 1);
		}
		successor.n += sibling.n;
		
		moveForwardKey(parent, index);
		moveForwardChild(parent, index + 1);
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
			node = loadChild(node, node.n);
		}
		return new Pair<>(node.key[node.n - 1], node.recordPos[node.n - 1]); 
	}
	
	private Pair<T,Long> findMinKey(Node<T> node) {
		while (!node.isLeaf) {
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
					assignKey(node, i, node, i + 1);
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
			Node<T> leftChild = loadChild(node, i);
			Node<T> rightChild = loadChild(node, i + 1);
	
			if (leftChild.n >= threshold) {
				Pair<T,Long> predKey = findMaxKey(leftChild);
				assignKeyValue(node, i, predKey.first, predKey.second);
				saveNode(node);
				return delete(leftChild, predKey.first);
			} else if (rightChild.n >= threshold) {
				Pair<T,Long> succKey = findMinKey(rightChild);
				assignKeyValue(node, i, succKey.first, succKey.second);
				saveNode(node);
				return delete(rightChild, succKey.first);
			} else {
				Node<T> successor = merge(node, i, leftChild, rightChild);	
				return delete(successor, key);
			}
		} else { //(i < node.n && comparator.compare(key, node.key[i]) < 0) || (i == node.n)
			Node<T> successor = loadChild(node, i);

			if (successor.n < threshold) {
				Node<T> rightSibling = (i < node.n) ? loadChild(node, i + 1) : null;
				Node<T> leftSibling = (i > 0) ? loadChild(node, i - 1) : null;
				
				if (rightSibling != null && rightSibling.n >= threshold) {
					//transplant : parent -> successor
					assignKey(successor, successor.n, node, i);
					successor.n++;
					
					//transplant : sibling -> parent
					assignKey(node, i, rightSibling, 0);
					if (!rightSibling.isLeaf) {
						assignChild(successor, successor.n, rightSibling, 0);
					}
					
					//moving all elements one by one left-hand side
					moveForwardKey(rightSibling, 0);
					if (!rightSibling.isLeaf) {
						moveForwardChild(rightSibling, 0);
					}
					rightSibling.n--;
					
					saveNode(node);
					saveNode(successor);
					saveNode(rightSibling);
				} else if (leftSibling != null && leftSibling.n >= threshold) {
					//move all elements one by one left-hand side
					moveBackKey(successor, 0);
					if (!successor.isLeaf) {
						moveBackChild(successor, 0);
					}
					successor.n++;
					
					//transplant : parent -> successor
					assignKey(successor, 0, node, i - 1);
					
					//transplant : left sibling -> parent
					assignKey(node, i - 1, leftSibling, leftSibling.n - 1);
					if (!leftSibling.isLeaf) {
						assignChild(node, 0, leftSibling, leftSibling.n);
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
				printSequentialOrder(loadChild(node, index));
				System.out.print(node.key[index] + " ");
				++index;
			}
			printSequentialOrder(loadChild(node, index));
		}
	}
	
	public void printSequentialOrder() {
		printSequentialOrder(root);
		System.out.println();
	}
}
