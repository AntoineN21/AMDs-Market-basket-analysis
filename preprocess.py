import pandas as pd
import re
import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords
# from nltk.stem import PorterStemmer
from multiprocessing import Pool, freeze_support, cpu_count
from tqdm import tqdm
import pickle as pkl

# Text preprocessing
stop_words = set(stopwords.words('english'))

def preprocess_text(text):
    # Remove special characters and convert to lowercase
    text = re.sub('[^a-zA-Z]', ' ', text.lower())
    # Tokenize the text into words
    words = text.split()
    # Remove stopwords
    words = [w for w in words if not w in stop_words]
    return words

def mapper(chunk):
    transactions = []
    for index, row in chunk.iterrows():
        text = row['text']
        words = preprocess_text(text)
        transactions.append(words)
    return transactions

def reducer(results):
    transactions = []
    for result in results:
        transactions.extend(result)
    return transactions

if __name__ == '__main__':
    freeze_support()

    # Create transactions
    transactions = []

    # Chunk size for loading the dataset
    chunk_size = 10000

    # Load the dataset in chunks
    chunks = pd.read_json('./market-basket-analysis/yelp_academic_dataset_review.json', lines=True, chunksize=chunk_size, nrows=1000000)

    # Determine the number of CPU cores available
    num_cores = cpu_count()

    # Create a pool of processes
    pool = Pool(num_cores)

    # Map phase: Process the chunks in parallel with a progress bar
    mapped_results = []
    for result in tqdm(pool.imap_unordered(mapper, chunks), total=num_cores):
        mapped_results.append(result)

    # Reduce phase: Concatenate the results from different processes
    transactions = reducer(mapped_results)

    # Close the pool of processes
    pool.close()
    pool.join()

    # # Print the transactions
    # for i, transaction in enumerate(transactions, start=1):
    #     print(f"Transaction {i}: {transaction}")

    # # Save the transactions to a file
    # with open('transactions.txt', 'w') as f:
    #     for transaction in transactions:
    #         f.write(' '.join(transaction) + '\n')

    # Save the transactions to a file (pkl)
    with open('transactions.pkl', 'wb') as f:
        pkl.dump(transactions, f)

    print("Number of transactions:", len(transactions))
