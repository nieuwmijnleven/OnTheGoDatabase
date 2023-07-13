package onthego.database.core.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

import org.junit.After;
import org.junit.Test;

import onthego.database.core.tablespace.manager.SingleTablespaceManager;
import onthego.database.core.tablespace.manager.TablespaceManager;
import onthego.database.core.tablespace.meta.SingleTablespaceHeader;
import onthego.database.core.tablespace.meta.TablespaceHeader;

public class BTreeIndexTest {
	
	private static final int MAX_KEY_VALUE = 20;
	
	private static final int MIN_KEY_VALUE = 1;
	
	private static final int BTREE_THRESHOLD = 128;
	
	private static final byte[] MAGIC = {0x11, 0x10, 0x09, 0x08, 0x07, 0x06, 0x05, 0x04};
	
	private static final String TABLESPACE_PATH = "./btree_index_tablespace.db";
	
	private BTreeIndex<Integer> btree;
	
	private TablespaceManager tsManager;
	
	@After
	public void tearDown() throws Exception {
		removeSingleTablespace();
	}
	
	private void createBTreeIndex() {
		try {
			createSingleTablespace();
			btree = new BTreeIndex<>(BTREE_THRESHOLD, tsManager);
		} catch(IOException ioe) {
			fail("failed to create a btree index.");
		}
	}
	
	private void createBTreeIndexWithData() {
		try {
			createSingleTablespace();
			btree = new BTreeIndex<>(BTREE_THRESHOLD, tsManager);
			for (int key = MIN_KEY_VALUE; key <= MAX_KEY_VALUE; ++key) {
				btree.insert(key, key + 1);
			}
			//printLevelOrder();
		} catch(IOException ioe) {
			fail("failed to create a btree index.");
		}
	}
	

	private void createSingleTablespace() throws IOException {
		removeSingleTablespace();
		
		TablespaceHeader tsHeader = new SingleTablespaceHeader.Builder()
										.magic(MAGIC)
										.chunkSize(16)
										.crc(10)
										.firstBlockPos(0)
										.firstFreeBlockPos(0)
										.tableRootPos(0)
										.tableMetaInfoPos(0)
										.recordCount(0)
										.build();
		
		tsManager = SingleTablespaceManager.create(TABLESPACE_PATH, tsHeader);
	}

	private void removeSingleTablespace() {
		if (tsManager != null) {
			tsManager.close();
		}
		
		File file = new File(TABLESPACE_PATH);
		if (file.exists()) {
			file.delete();
		}
	}
	
	@Test
	public void testContains() {
		createBTreeIndex();
		
		for (int key = MIN_KEY_VALUE; key <= MAX_KEY_VALUE; ++key) {
			assertFalse(btree.contains(key));
			btree.insert(key, key + 1);
			assertTrue(btree.contains(key));
		}
		
		assertFalse(btree.contains(MAX_KEY_VALUE + 1));
		assertFalse(btree.contains(0));
	}

	@Test
	public void testForwardSequentialInsert() {
		createBTreeIndex();
		
		for (int key = MIN_KEY_VALUE; key <= MAX_KEY_VALUE; ++key) {
			assertFalse(btree.contains(key));
			btree.insert(key, key + 1);
			assertTrue(btree.contains(key));
		}
	}
	
	@Test
	public void testBackwardSequentialInsert() {
		createBTreeIndex();
		
		for (int key = MAX_KEY_VALUE; key <= MIN_KEY_VALUE; --key) {
			assertFalse(btree.contains(key));
			btree.insert(key, key + 1);
			assertTrue(btree.contains(key));
		}
	}
	
	@Test
	public void testRandomInsert() {
		createBTreeIndex();
		
		int[] used = new int[(MAX_KEY_VALUE - MIN_KEY_VALUE + 32) / 32];
		for (int i = MIN_KEY_VALUE; i <= MAX_KEY_VALUE; ++i) {
			int randomKey = getUnusedRandomNumeberInRange(MIN_KEY_VALUE, MAX_KEY_VALUE, used);
			assertFalse(btree.contains(randomKey));
			btree.insert(randomKey, randomKey + 1);
			assertTrue(btree.contains(randomKey));
		}
	}
	
	@Test
	public void testForwardSequentialDelete() {
		createBTreeIndexWithData();
		
		for (int i = MIN_KEY_VALUE; i <= MAX_KEY_VALUE; ++i) {
			assertTrue(btree.contains(i));
			btree.delete(i);
			checkOrder();
			assertFalse(btree.contains(i));
		}
	}
	
	@Test
	public void testBackwardSequentialDelete() {
		createBTreeIndexWithData();
		
		for (int i = MAX_KEY_VALUE; i >= MIN_KEY_VALUE; --i) {
			assertTrue(btree.contains(i));
			btree.delete(i);
			checkOrder();
			assertFalse(btree.contains(i));
		}
	}	
	
	@Test
	public void testRandomDelete() {
		createBTreeIndexWithData();
		
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
		createBTreeIndexWithData();
		
		int key = MIN_KEY_VALUE;
		Iterator<Integer> it = btree.iterator();
		while (it.hasNext()) {
			assertTrue(key++ == it.next());
		}
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
			if (prevKey >= curKey) {
				System.out.printf("prevKey = %d, curKey = %d\n", prevKey, curKey);
				btree.printLevelOrder();
			}
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
