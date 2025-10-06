package onthego.database.core.index;

import onthego.database.core.exception.InsufficientPayloadSpaceException;
import onthego.database.core.serializer.LongSerializer;
import onthego.database.core.serializer.Serializer;
import onthego.database.core.table.meta.ColumnMeta;
import onthego.database.core.table.meta.Type;
import onthego.database.core.tablespace.manager.TablespaceManager;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;

import static java.util.stream.Collectors.joining;

public class BPlusTreeIndex<T> {

	private final int threshold;

	private final Comparator<T> comparator;

    private final Serializer<T> serializer;

	private final TablespaceManager tsManager;

	private Node<T> root;

	private int estimatedNodeSize;

    public BPlusTreeIndex(int threshold, TablespaceManager tsManager) {
        this(threshold, null, null, tsManager);
        initialize();
    }

	@SuppressWarnings("unchecked")
    public BPlusTreeIndex(int threshold, Serializer<?> serializer, Comparator<?> comparator, TablespaceManager tsManager) {
        if (serializer == null || comparator == null) {
            List<ColumnMeta> columMetaList = tsManager.getHeader().getTableMetaInfo().getColumnList();
            Optional<ColumnMeta> primaryKeyColumnMeta = columMetaList.stream().filter(ColumnMeta::isKey).findFirst();
            if (primaryKeyColumnMeta.isPresent()) {
                Type primaryKeyType = primaryKeyColumnMeta.get().getType();
                serializer = primaryKeyType.getSerializer();
                comparator = primaryKeyType.getComparator();
            } else {
                serializer = new LongSerializer();
                comparator = Comparator.comparingLong(Long.class::cast);
            }
        }

        this.threshold = threshold;
        this.serializer = (Serializer<T>) serializer;
        this.comparator = (Comparator<T>) comparator;
        this.tsManager = tsManager;
        this.estimatedNodeSize = estimateNodeSize();
        initialize();
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
        try (
                ByteArrayOutputStream bout = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(bout)
        ) {
            out.writeBoolean(node.isLeaf);
            out.writeInt(node.n);

            if (node.isLeaf) {
                out.writeLong(node.prevLeafRecordPos);
                out.writeLong(node.nextLeafRecordPos);
            }

            for (int i = 0; i < node.key.length; ++i) {
                serializer.write(out, node.key[i]);
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
        final Node<T> node = new Node<>(threshold, 0);
        final int Boolean_BYTES = 1;
        return Boolean_BYTES + Integer.BYTES + (serializer.estimateSize(null) * node.key.length)  + (Long.BYTES * node.recordPos.length) + (Long.BYTES * node.childPos.length);
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
			 DataInputStream in = new DataInputStream(bin)) {
			Node<T> node = new Node<>(threshold, pos);
			node.isLeaf = in.readBoolean();
			node.n = in.readInt();

            if (node.isLeaf) {
                node.prevLeafRecordPos = in.readLong();
                node.nextLeafRecordPos = in.readLong();
            }
			
			for (int i = 0; i < node.key.length; ++i) {
				node.key[i] = serializer.read(in);
			}
			
			for (int i = 0; i < node.recordPos.length; ++i) {
				node.recordPos[i] = in.readLong();
			}
			
			for (int i = 0; i < node.childPos.length; ++i) {
				node.childPos[i] = in.readLong();
			}
			
			return node;
		} catch(Exception e) {
			throw new BTreeIndexException(e);
		}
	}
	
	private Node<T> allocateNode(boolean isLeaf) {
		long pos = tsManager.allocate(this.estimatedNodeSize);
		return new Node<>(threshold, isLeaf, pos);
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

        if (node.isLeaf) {
            if (i < node.n && comparator.compare(key, node.key[i]) == 0) { //equal to the key
                return node.recordPos[i];
            } else { // could not find the key in the leaf node, there is no the key in BTree
                return -1;
            }
        } else {
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
        System.arraycopy(src.key, srcIndex, dest.key, destIndex, count);
        System.arraycopy(src.recordPos, srcIndex, dest.recordPos, destIndex, count);
    }
	
	private void transplantChild(Node<T> dest, int destIndex, Node<T> src, int srcIndex, int count) {
        System.arraycopy(src.child, srcIndex, dest.child, destIndex, count);
        System.arraycopy(src.childPos, srcIndex, dest.childPos, destIndex, count);
	}

    private void moveBackKey(Node<T> node, int from) {
        int length = node.n - from;
        System.arraycopy(node.key, from, node.key, from + 1, length);
        System.arraycopy(node.recordPos, from, node.recordPos, from + 1, length);
    }

    private void moveBackChild(Node<T> node, int from) {
        int length = node.n - from + 1;
        System.arraycopy(node.child, from, node.child, from + 1, length);
        System.arraycopy(node.childPos, from, node.childPos, from + 1, length);
    }

    private void moveForwardKey(Node<T> node, int to) {
        int length = node.n - to - 1;
        System.arraycopy(node.key, to + 1, node.key, to, length);
        System.arraycopy(node.recordPos, to + 1, node.recordPos, to, length);
    }

    private void moveForwardChild(Node<T> node, int to) {
        int length = node.n - to;
        System.arraycopy(node.child, to + 1, node.child, to, length);
        System.arraycopy(node.childPos, to + 1, node.childPos, to, length);
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
        successor.n = threshold;
		
		moveBackKey(parent, index);
		moveBackChild(parent, index + 1);
		assignChildValue(parent, index + 1, sibling, sibling.pos);
		assignKey(parent, index, sibling, 0);
        ++parent.n;

        if (successor.isLeaf) {
            if (successor.nextLeaf != null) {
                successor.nextLeaf.prevLeaf = sibling;
                successor.nextLeaf.prevLeafRecordPos = sibling.pos;
            }
            sibling.nextLeaf = successor.nextLeaf;
            sibling.nextLeafRecordPos = successor.nextLeafRecordPos;
            successor.nextLeaf = sibling;
            successor.nextLeafRecordPos = sibling.pos;
            sibling.prevLeaf = successor;
            sibling.prevLeafRecordPos = successor.pos;
        }

        saveNode(parent);
		saveNode(successor);
		saveNode(sibling);
	}

	private void insert(Node<T> node, T key, long recordPos) {
        int index = node.n - 1;
        if (node.isLeaf) {
            while (index >= 0 && comparator.compare(key, node.key[index]) <= 0) {
				assignKey(node, index + 1, node, index);
				--index;
			}
			
			assignKeyValue(node, index + 1, key, recordPos);
			++node.n;
			saveNode(node);
		} else {
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

    public boolean delete(final T key) {
        boolean result = delete(null, this.root, key);
        if (this.root.n == 0 && !this.root.isLeaf) {
            Node<T> removedRoot = this.root;
            this.root = loadChild(removedRoot,0);
            this.root.pos = removedRoot.childPos[0];
            tsManager.saveRootPos(this.root.pos);
            freeNode(removedRoot);
        }
        return result;
    }

    private boolean delete(final Node<T> parent, final Node<T> node, final T key) {
        // find i such that node.key[i-1] <= key <= node.key[i]
        int i = 0;
        while (i < node.n && comparator.compare(key, node.key[i]) > 0) {
            ++i;
        }

        // in case of reaching to leaf node
        if (node.isLeaf) {
            if (i < node.n && comparator.compare(key, node.key[i]) == 0) {
                moveForwardKey(node, i);
                node.n--;
                saveNode(node);
                return true;
            }
            return false;
        } else {
            Node<T> child = loadChild(node, i);
            boolean result = delete(node, child, key);

            if (node.child[i].n < threshold - 1) {
                fixUnderflow(node, node.child[i]);
            }
            return result;
        }
    }

    private void fixUnderflow(final Node<T> parent, final Node<T> node) {
        int i = findChildIndex(parent, node);
        if (i == -1) {
            throw new BPlusTreeIndexException("cannot find a child node from a parent node.");
        }
        // borrow from left, borrow from right or merge nodes.
        if (i > 0 && parent.child[i-1].n > threshold - 1) {
            borrowFromLeft(parent, node, i);
        } else if (i < parent.n && parent.child[i+1].n > threshold -1) {
            borrowFromRight(parent, node, i);
        } else {
            if (i > 0) merge(parent, i-1);
            else merge(parent, i);
        }
    }

    private void merge(Node<T> parent, int i) {
        Node<T> leftSibling = loadChild(parent, i);
        Node<T> rightSibling = loadChild(parent, i+1);

        if (leftSibling.isLeaf) {
            transplantKey(leftSibling, leftSibling.n, rightSibling, 0, rightSibling.n);
            leftSibling.n = leftSibling.n + rightSibling.n;

            if (rightSibling.nextLeaf != null) {
                rightSibling.nextLeaf.prevLeaf = leftSibling;
                rightSibling.nextLeaf.prevLeafRecordPos = leftSibling.pos;
            }
            leftSibling.nextLeaf = rightSibling.nextLeaf;
            leftSibling.nextLeafRecordPos = rightSibling.nextLeafRecordPos;
        } else {
            leftSibling.key[leftSibling.n] = parent.key[i];
            transplantKey(leftSibling, leftSibling.n+1, rightSibling, 0, rightSibling.n);
            transplantChild(leftSibling, leftSibling.n+1, rightSibling, 0, rightSibling.n+1);
            leftSibling.n = leftSibling.n + 1 + rightSibling.n;
        }

        moveForwardKey(parent, i);
        moveForwardChild(parent, i+1);
        --parent.n;

        saveNode(parent);
        saveNode(leftSibling);
        freeNode(rightSibling);
    }

    private void borrowFromRight(Node<T> parent, Node<T> node, int i) {
        Node<T> rightSibling = loadChild(parent, i+1);
        if (node.isLeaf) {
            node.key[node.n] = rightSibling.key[0];
            node.recordPos[node.n] = rightSibling.recordPos[0];
            ++node.n;

            moveForwardKey(rightSibling, 0);
            --rightSibling.n;

            parent.key[i] = rightSibling.key[0];
        } else {
            node.key[node.n] = parent.key[i];
            node.child[node.n] = loadChild(rightSibling, 0);
            node.childPos[node.n] = rightSibling.childPos[0];
            parent.key[i] = rightSibling.key[0];
            ++node.n;

            moveForwardKey(rightSibling, 0);
            moveForwardChild(rightSibling, 0);
            --rightSibling.n;
        }

        saveNode(parent);
        saveNode(rightSibling);
        saveNode(node);
    }

    private void borrowFromLeft(Node<T> parent, Node<T> node, int i) {
        Node<T> leftSibling = loadChild(parent, i-1);
        if (node.isLeaf) {
            moveBackKey(node, 0);
            ++node.n;

            node.key[0] = leftSibling.key[leftSibling.n-1];
            node.recordPos[0] = leftSibling.recordPos[leftSibling.n-1];
            --leftSibling.n;

            parent.key[i-1] = node.key[0];
        } else {
            moveBackKey(node, 0);
            moveBackChild(node, 0);
            ++node.n;

            node.key[0] = parent.key[i-1];
            node.child[0] = loadChild(leftSibling, leftSibling.n);
            node.childPos[0] = leftSibling.childPos[leftSibling.n];
            parent.key[i-1] = leftSibling.key[leftSibling.n-1];
            --leftSibling.n;
        }

        saveNode(parent);
        saveNode(leftSibling);
        saveNode(node);
    }



    private int findChildIndex(final Node<T> parent, final Node<T> node) {
        for (int i = 0; i <= parent.n; ++i) {
            if (parent.child[i] == node) {
                return i;
            }
        }
        return -1;
    }
	
	public Iterator<BTreeRecordInfo<T>> iterator() {
		return new BPlusTreeIterator(createSnapshot(this.root));
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
				
				if (node != null && !node.isLeaf) {
					for (int i = 0; i <= node.n; ++i) {
						queue.add(loadChild(node, i));
					}
				}
			}
			System.out.println();
		}
	}

    private Node<T> createSnapshot(final Node<T> node) {
        if (node == null) {
            throw new BTreeIndexException("cannot create a snapshot for a empty BTreeIndex.");
        }
        return _createSnapshot(node);
    }

    private Node<T> _createSnapshot(final Node<T> node) {
        Node<T> replica = Node.copyOf(node);
        if (node.isLeaf) {
            return replica;
        } else {
            int index = 0;
            while (index <= node.n) {
                replica.child[index] = createSnapshot(loadChild(node, index));
                ++index;
            }
        }
        return replica;
    }

    final static class BPlusTreeIterator<T> implements Iterator<BTreeRecordInfo<T>> {

        Node<T> currentNode;

        int currentIndex;

        BPlusTreeIterator(final Node<T> root) {
            if (root != null && root.n > 0) {
                this.currentNode = root;
                this.currentIndex = 0;

                while (!this.currentNode.isLeaf) {
                    this.currentNode = this.currentNode.child[0];
                }
            }
        }

        @Override
        public boolean hasNext() {
            return currentNode != null && currentIndex < currentNode.n;
        }

        @Override
        public BTreeRecordInfo<T> next() {
            if (!hasNext()) {
                return null;
            }

            BTreeRecordInfo<T> recordInfo = new BTreeRecordInfo<>(currentNode.key[currentIndex], currentNode.recordPos[currentIndex]);

            ++currentIndex;
            if (currentIndex >= currentNode.n) {
                currentNode = currentNode.nextLeaf;
                currentIndex = 0;
            }

            return recordInfo;
        }
    }

    final static class Node<T> {
        // a flag that check whether this node is a leaf or not.
        boolean isLeaf;
        // threshold-1 <= n <= 2*threshold-1
        int threshold;
        // the number of keys
        int n;
        // the position of the record of a tablespace related to this Node.
        long pos;

        // interal & terminal node
        T[] key;

        // terminal node
        long[] recordPos;

        // internal node
        Node<T>[] child;
        long[] childPos;

        // linked list for leaf nodes.
        Node<T> prevLeaf;
        Node<T> nextLeaf;

        // the position of the record related to leaf nodes
        long prevLeafRecordPos;
        long nextLeafRecordPos;

        static <T> Node<T> copyOf(final Node<T> node) {
            final Node<T> newNode = new Node<>(node.threshold, node.pos);

            newNode.isLeaf = node.isLeaf;
            newNode.n = node.n;
            newNode.prevLeaf = node.prevLeaf;
            newNode.nextLeaf = node.nextLeaf;

            System.arraycopy(node.key, 0, newNode.key, 0, node.key.length);
            System.arraycopy(node.recordPos, 0, newNode.recordPos, 0, node.recordPos.length);
            System.arraycopy(node.child, 0, newNode.child, 0, node.child.length);
            System.arraycopy(node.childPos, 0, newNode.childPos, 0, node.childPos.length);
            return newNode;
        }

        @SuppressWarnings("unchecked")
        Node(int threshold, long pos) {
            this.threshold = threshold;
            this.isLeaf = false;
            this.n = 0;
            this.key = (T[])new Comparable<?>[2*threshold - 1];
            this.recordPos = new long[2*threshold - 1];
            this.pos = pos;

            this.child = (Node<T>[])new Node<?>[2*threshold];
            this.childPos = new long[2*threshold];
        }

        Node(int threshold, boolean isLeaf, long pos) {
            this(threshold, pos);
            this.isLeaf = isLeaf;
        }

        Node(int threshold, boolean isLeaf, long pos, long prevLeafRecordPos, long nextLeafRecordPos) {
            this(threshold, pos);
            this.isLeaf = isLeaf;
            this.prevLeafRecordPos = prevLeafRecordPos;
            this.nextLeafRecordPos = nextLeafRecordPos;
        }

        @Override
        public String toString() {
            return Arrays.stream(key, 0, n).map(String::valueOf)
                    .collect(joining(",", "[", "]"));
        }
    }
}
