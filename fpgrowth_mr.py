from itertools import combinations
from collections import defaultdict
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
import pickle as pkl

def mapper(args):
    """
    Map phase: Generate conditional patterns for each chunk

    Parameters:
    ----------
    args : tuple
        Tuple containing the chunk, frequent single items, and minimum support

    Returns:
    -------
    mapped_patterns : dict
        Dictionary containing the frequent items and their conditional patterns
    """
    chunk, frequent_single_items, min_support = args
    mapped_patterns = defaultdict(list)

    for transaction in chunk:
        items = set(transaction)
        frequent_items_in_transaction = items & frequent_single_items

        # Generate conditional patterns for frequent items in the transaction
        for item in frequent_items_in_transaction:
            pattern = [i for i in transaction if i != item]
            if pattern:
                mapped_patterns[item].append(pattern)
    

    return mapped_patterns

def reducer(mapped_data, frequent_single_items, item_counts, min_support):
    """
    Reduce phase: Combine conditional patterns from different processes

    Parameters:
    ----------
    mapped_data : list
        List containing the mapped data from different processes
    frequent_single_items : set
        Set of frequent single items
    item_counts : dict
        Dictionary containing the counts of each item
    min_support : int
        Minimum support threshold

    Returns:
    -------
    frequent_itemsets : list
        List containing the frequent itemsets
    """
    frequent_itemsets = []

    conditional_patterns = defaultdict(list)

    for mapped_patterns in mapped_data:
        for item, patterns in mapped_patterns.items():
            conditional_patterns[item].extend(patterns)

    # Mine frequent itemsets using the conditional patterns
    frequent_itemsets = mine_frequent_itemsets(frequent_single_items, item_counts, min_support, conditional_patterns)

    print("Frequent Itemsets:")
    for itemset, support in frequent_itemsets:
        print(itemset, "Support:", support)

    return frequent_itemsets

def generate_conditional_patterns(conditional_patterns, prefix):
    """
    Generate conditional patterns for a given prefix from the conditional pattern base

    Parameters:
    ----------
    conditional_patterns : dict
        Dictionary containing the conditional patterns for each frequent item
    prefix : tuple
        Prefix for the itemset

    Returns:
    -------
    new_conditional_patterns : dict
        Dictionary containing the conditional patterns for the given prefix
    """
    new_conditional_patterns = defaultdict(list)
    for item, patterns in conditional_patterns.items():
        for pattern in patterns:
            if set(prefix).issubset(set(pattern)):  # Check if the prefix is a subset of the pattern
                new_pattern = [i for i in pattern if i not in prefix]
                if new_pattern:
                    new_conditional_patterns[item].append(new_pattern)
    return new_conditional_patterns



def generate_frequent_items(conditional_patterns, min_support):
    item_counts = defaultdict(int)
    for item, patterns in conditional_patterns.items():
        count = sum(1 for pattern in patterns)
        if count >= min_support:
            item_counts[item] = count
    frequent_items = set(item_counts.keys())
    return frequent_items, item_counts


def mine_frequent_itemsets(frequent_items, item_counts, min_support, conditional_patterns, prefix=None):
    if prefix is None:
        prefix = []
    
    frequent_itemsets = []
    for item in frequent_items:
        updated_prefix = prefix + [item]
        support = item_counts.get(tuple(updated_prefix), 0)
        if support >= min_support:
            frequent_itemsets.append((tuple(updated_prefix), support))
            new_conditional_patterns = generate_conditional_patterns(conditional_patterns, list(updated_prefix))
            new_frequent_items, new_item_counts = generate_frequent_items(new_conditional_patterns, min_support)
            frequent_itemsets.extend(mine_frequent_itemsets(new_frequent_items, new_item_counts, min_support, new_conditional_patterns, prefix=updated_prefix))
    
    return frequent_itemsets









def construct_conditional_tree(pattern_base):
    """
    Construct the conditional FP-tree from the pattern base

    Parameters:
    ----------
    pattern_base : list
        List of patterns in the pattern base

    Returns:
    -------
    tree : FPNode
        Root node of the conditional FP-tree
    item_counts : dict
        Dictionary containing the counts of each item in the conditional FP-tree
    """
    item_counts = defaultdict(int)
    tree = FPNode(None, None)

    for pattern in pattern_base:
        current_node = tree
        for item in pattern:
            child_node = current_node.get_child(item)
            if child_node is None:
                child_node = FPNode(item, current_node)
                current_node.add_child(child_node)
            current_node = child_node
            item_counts[item] += 1

    return tree, item_counts

class FPNode:
    def __init__(self, item, parent):
        self.item = item
        self.parent = parent
        self.children = []
        self.next = None
        self.count = 1

    def add_child(self, child):
        self.children.append(child)

    def get_child(self, item):
        for child in self.children:
            if child.item == item:
                return child
        return None

def FP_growth(transactions, min_support):
    """
    FP-growth algorithm

    Parameters:
    ----------
    transactions : list
        List of transactions
    min_support : int
        Minimum support threshold

    Returns:
    -------
    frequent_itemsets : list
        List containing the frequent itemsets
    """
    # First pass - Counting frequent single items
    single_item_counts = defaultdict(int)
    for transaction in transactions:
        for item in transaction:
            single_item_counts[item] += 1

    frequent_single_items = set([item for item, count in single_item_counts.items() if count >= min_support])
    item_counts = dict(single_item_counts)

    # Divide transactions into chunks
    num_processes = cpu_count()
    chunk_size = len(transactions) // num_processes
    chunks = [transactions[i:i+chunk_size] for i in range(0, len(transactions), chunk_size)]
    args = [(chunk, frequent_single_items, min_support) for chunk in chunks]

    # Create a pool of worker processes
    pool = Pool(num_processes)

    # Map phase with progress bar
    mapped_data = list(tqdm(pool.imap(mapper, args), total=len(args), desc="Mapping"))

    # Reduce phase with progress bar
    frequent_itemsets = list(tqdm(reducer(mapped_data, frequent_single_items, item_counts, min_support), desc="Reducing"))

    # Close the pool of worker processes
    pool.close()
    pool.join()

    return frequent_itemsets

if __name__ == '__main__':
    transactions = []
    with open('transactions.pkl', 'rb') as f:
        transactions = pkl.load(f)

    min_support = 0
    frequent_itemsets = FP_growth(transactions[:10000], min_support)
    print("Frequent Itemsets:")
    for itemset, support in frequent_itemsets:
        print(itemset, "Support:", support)
