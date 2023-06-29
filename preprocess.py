import pandas as pd
import re
import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords
from multiprocessing import Pool, cpu_count
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
    words = [w for w in words if w not in stop_words]
    return words

def process_chunk(chunk):
    transactions = []
    for _, row in chunk.iterrows():
        text = row['text']
        words = preprocess_text(text)
        transactions.append(words)
    return transactions

if __name__ == '__main__':
    # Load the dataset
    chunk_size = 10000
    nrows = 1000000
    chunks = pd.read_json('./AMDs-Market-basket-analysis/yelp_academic_dataset_review.json', lines=True, chunksize=chunk_size, nrows=nrows)

    # Determine the number of CPU cores available
    num_cores = cpu_count()

    # Create a pool of processes
    with Pool(num_cores) as pool:
        # Map phase: Process the chunks in parallel with a progress bar
        mapped_results = list(tqdm(pool.imap_unordered(process_chunk, chunks), total=nrows // chunk_size))

    # Reduce phase: Concatenate the results from different processes
    transactions = [transaction for result in mapped_results for transaction in result]

    # Save the transactions to a file (pkl)
    with open('transactions.pkl', 'wb') as f:
        pkl.dump(transactions, f)

    print("Number of transactions:", len(transactions))
