from mrjob.job import MRJob
from mrjob.step import MRStep
from collections import defaultdict
from itertools import combinations


class FPGrowthMR(MRJob):

    def configure_args(self):
      super(FPGrowthMR, self).configure_args()
      self.add_passthru_arg('--min_support', type=int, default=2)
      self.add_passthru_arg('--min_confidence', type=float, default=0.5)


    def mapper(self, _, line):
        transaction = line.strip().split(',')
        for item in transaction:
          yield item, 1

    def reducer(self, item, counts):
        support = sum(counts)
        if support >= self.options.min_support:
          yield None, (item, support)

  
    def find_frequent_itemsets(self, _, item_counts):
        item_counts = list(item_counts)
        frequent_items = sorted(item_counts, key=lambda x: x[1], reverse=True)
        for itemset, support in frequent_items:
          yield itemset, support


    def find_frequent_itemsets(self, _, item_counts):
        item_counts = list(item_counts)
        frequent_items = sorted(item_counts, key=lambda x: x[1], reverse=True)
        for itemset, support in frequent_items:
            yield itemset, support

    def generate_association_rules(self, itemset, support):
        if len(itemset) < 2:
            return

        itemset = sorted(itemset)
        for i in range(1, len(itemset)):
            for antecedent in combinations(itemset, i):
                antecedent = list(antecedent)
                consequent = list(set(itemset) - set(antecedent))
                confidence = support / self.item_counts.get(tuple(antecedent), 0)
                if confidence >= self.options.min_confidence:
                    yield antecedent, consequent, support, confidence

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(reducer_init=self.reducer_init,
                   reducer=self.find_frequent_itemsets),
            MRStep(reducer=self.generate_association_rules)
        ]

    def reducer_init(self):
        self.item_counts = defaultdict(int)

    def reducer(self, _, item_counts):
        for item, support in item_counts:
            self.item_counts[item] += support

   if __name__ == '__main__':
    FPGrowthMR.run()
