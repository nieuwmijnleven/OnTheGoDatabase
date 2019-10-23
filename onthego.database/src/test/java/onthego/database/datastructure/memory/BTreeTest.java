package onthego.database.datastructure.memory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import onthego.database.core.datastructure.memory.BTree;

public class BTreeTest {
	
	private static final int MAX_KEY_VALUE = 10000;
	
	private static final int MIN_KEY_VALUE = 1;
	
	private static final int BTREE_THRESHOLD = 128;
	
	private BTree<Integer> btree;
	
	@Test
	public void testContains() {
		btree = new BTree<>(BTREE_THRESHOLD);
		for (int key = MIN_KEY_VALUE; key <= MAX_KEY_VALUE; ++key) {
			assertFalse(btree.contains(key));
			btree.insert(key);
			assertTrue(btree.contains(key));
		}
		
		assertFalse(btree.contains(MAX_KEY_VALUE + 1));
		assertFalse(btree.contains(0));
	}

	@Test
	public void testForwardSequentialInsert() {
		btree = new BTree<>(BTREE_THRESHOLD);
		for (int key = MIN_KEY_VALUE; key <= MAX_KEY_VALUE; ++key) {
			assertFalse(btree.contains(key));
			btree.insert(key);
			assertTrue(btree.contains(key));
		}
	}
	
	@Test
	public void testBackwardSequentialInsert() {
		btree = new BTree<>(BTREE_THRESHOLD);
		for (int key = MAX_KEY_VALUE; key <= MIN_KEY_VALUE; --key) {
			assertFalse(btree.contains(key));
			btree.insert(key);
			assertTrue(btree.contains(key));
		}
	}
	
	@Test
	public void testRandomInsert() {
		btree = new BTree<>(BTREE_THRESHOLD);
		int[] used = new int[(MAX_KEY_VALUE - MIN_KEY_VALUE + 32) / 32];
		
		for (int i = MIN_KEY_VALUE; i <= MAX_KEY_VALUE; ++i) {
			int randomKey = getUnusedRandomNumeberInRange(MIN_KEY_VALUE, MAX_KEY_VALUE, used);
			assertFalse(btree.contains(randomKey));
			btree.insert(randomKey);
			assertTrue(btree.contains(randomKey));
		}
	}
	
	@Test
	public void testForwardSequentialDelete() {
		createBTree();
		for (int i = MIN_KEY_VALUE; i <= MAX_KEY_VALUE; ++i) {
			assertTrue(btree.contains(i));
			btree.delete(i);
			checkOrder();
			assertFalse(btree.contains(i));
		}
	}
	
	@Test
	public void testBackwardSequentialDelete() {
		createBTree();
		for (int i = MAX_KEY_VALUE; i >= MIN_KEY_VALUE; --i) {
			assertTrue(btree.contains(i));
			btree.delete(i);
			checkOrder();
			assertFalse(btree.contains(i));
		}
	}	
	
	@Test
	public void testRandomDelete() {
		createBTree();
		int[] used = new int[(MAX_KEY_VALUE - MIN_KEY_VALUE + 32) / 32];
		for (int i = MIN_KEY_VALUE; i <= MAX_KEY_VALUE; ++i) {
			int randomKey = getUnusedRandomNumeberInRange(MIN_KEY_VALUE, MAX_KEY_VALUE, used);
			btree.delete(randomKey);
			checkOrder();
			assertFalse(btree.contains(randomKey));
		}
	}

	@Test
	public void testIterator() {
		createBTree();
		
		int key = MIN_KEY_VALUE;
		Iterator<Integer> it = btree.iterator();
		while (it.hasNext()) {
			assertTrue(key++ == it.next());
		}
	}
	
	private void createBTree() {
		btree = new BTree<>(BTREE_THRESHOLD);
		for (int key = MIN_KEY_VALUE; key <= MAX_KEY_VALUE; ++key) {
			btree.insert(key);
		}
		//printLevelOrder();
	}
	
	private void printLevelOrder() {
		System.out.println("Print Level Order => ");
		btree.printLevelOrder();
	}

	private void checkOrder() {
		Iterator<Integer> it = btree.iterator();
		
		int prevKey = 0;
		while (it.hasNext()) {
			int curKey = it.next();
			//if (prevKey >= curKey) {
			//	System.out.printf("prevKey = %d, curKey = %d\n", prevKey, curKey);
			//}
			assertTrue(prevKey < curKey);
			prevKey = curKey;
		}
	}
	
	private void countKeys(int expected) {
		Iterator<Integer> it = btree.iterator();
		
		int counter = 0;
		while (it.hasNext()) {
			it.next();
			counter++;
		}
		
		assertEquals(expected, counter);
		//System.out.println("Checking the number of keys => " + counter);
	}
	
	private int getRandomNumberInRange(int min, int max) {
		Random random = new Random();
		return random.ints(min, (max + 1)).limit(1).findFirst().getAsInt();
	}
	
	private int getUnusedRandomNumeberInRange(int min, int max, int[] used) {
		int randVal = min;
		do {
			randVal = getRandomNumberInRange(min, max);
		} while ((used[(randVal - min) / 32] & (1 << ((randVal - min) % 32))) != 0);
		
		used[(randVal - min) / 32] |= 1 << ((randVal - min) % 32);
		return randVal;
	}
}
