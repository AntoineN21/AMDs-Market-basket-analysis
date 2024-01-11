from itertools import combinations
import numpy as np
from multiprocessing import Pool
import multiprocessing
from tqdm import tqdm
import time
import pickle

def map_function(args):
    """
    Map function for the PCY algorithm. Counts candidate pairs within a transaction chunk.

    Parameters:
    args (tuple): Contains a chunk of transactions, the set of frequent single items, and the hash table size.

    Returns:
    tuple: A dictionary of candidate counts and a hash table array.
    """
    chunk, frequent_items, size_of_hash_table = args
    pair_counts = {}
    hash_table = np.zeros(size_of_hash_table, dtype=int)

    for transaction in chunk:
        items = set(transaction)
        candidate_pairs = [pair for pair in combinations(items, 2) if pair[0] in frequent_items and pair[1] in frequent_items]

        for pair in candidate_pairs:
            hash_index = hash(pair) % size_of_hash_table
            hash_table[hash_index] += 1
            pair_counts[pair] = pair_counts.get(pair, 0) + 1

    return pair_counts, hash_table

def reduce_function(mapped_results, support_threshold, hash_table_size):
    """
    Reduce function for the PCY algorithm. Filters and aggregates the frequent itemsets.

    Parameters:
    mapped_results (list): List of tuples from the map function containing candidate counts and hash tables.
    support_threshold (int): Minimum support threshold for itemsets.
    hash_table_size (int): Size of the hash table.

    Returns:
    list: Frequent itemsets that meet the support threshold.
    """
    total_pair_counts = {}
    total_hash_table = np.zeros(hash_table_size, dtype=int)

    for itemset_counts, hash_table in mapped_results:
        for itemset, count in itemset_counts.items():
            total_pair_counts[itemset] = total_pair_counts.get(itemset, 0) + count
            hash_index = hash(itemset) % hash_table_size
            total_hash_table[hash_index] += count

    return [(itemset, count) for itemset, count in total_pair_counts.items() if count >= support_threshold and total_hash_table[hash(itemset) % hash_table_size] >= support_threshold]

def run_PCY_algorithm(transactions, support_threshold, hash_table_size, chunk_size):
    """
    Executes the PCY algorithm to find frequent itemsets in transaction data.

    Parameters:
    transactions (list): Transaction data.
    support_threshold (int): Minimum support threshold for itemsets.
    hash_table_size (int): Size of the hash table.
    chunk_size (int): Size of each transaction chunk.

    Returns:
    list: Frequent itemsets found in the transactions.
    """
    # Counting frequent single items
    item_counts = {item: 0 for transaction in transactions for item in transaction}
    for transaction in transactions:
        for item in transaction:
            item_counts[item] += 1

    frequent_items = {item for item, count in item_counts.items() if count >= support_threshold}

    # Split transactions into chunks for parallel processing
    transaction_chunks = [transactions[i:i + chunk_size] for i in range(0, len(transactions), chunk_size)]
    chunk_args = [(chunk, frequent_items, hash_table_size) for chunk in transaction_chunks]

    # Parallel map phase
    print(f"Number of worker processes: {multiprocessing.cpu_count()}")
    with Pool() as pool:
        map_results = list(tqdm(pool.imap(map_function, chunk_args), total=len(chunk_args), desc="Map Phase"))

    # Reduce phase
    frequent_itemsets = list(tqdm(reduce_function(map_results, support_threshold, hash_table_size), desc="Reduce Phase"))

    return frequent_itemsets


if __name__ == '__main__':
    # Load transactions from a file
    with open('transactions.pkl', 'rb') as file:
        transactions = pickle.load(file)

    print(f"Number of transactions: {len(transactions)}")
    transactions_subset = transactions[:500000]
    print("Using only the first 500000 transactions")

    # Parameters for the PCY algorithm
    min_support = 10000
    hash_table_size = 1000
    chunk_size = 10000

    # Performance evaluation
    execution_times = []
    subset_sizes = [1000, 10000, 100000, 200000]
    
    for size in subset_sizes:
        print(f"Processing subset size: {size}")
        start_time = time.time()
        itemsets = run_PCY_algorithm(transactions_subset[:size], min_support, hash_table_size, chunk_size)
        end_time = time.time()
        execution_times.append(end_time - start_time)
        print(f"Time taken: {end_time - start_time}\n")

    # Save performance metrics and frequent itemsets to files
    with open("pcy_times.pkl", "wb") as file:
        pickle.dump(execution_times, file)
    with open("frequent_itemsets_pcy.pkl", "wb") as file:
        pickle.dump(itemsets, file)

    print(f"Number of frequent itemsets: {len(itemsets)}")
