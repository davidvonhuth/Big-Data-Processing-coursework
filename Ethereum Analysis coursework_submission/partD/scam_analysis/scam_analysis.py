from mrjob.job import MRJob
from mrjob.step import MRStep

class scam_analysis(MRJob):
    def mapper1(self, _,line):
        try:
            if(len(line.split(',')) == 7):   #TRANSACTIONS, which can be a transfer to another user (perhaps fraudulent), or a payment for use of a smart contract
                fields = line.split(',')
                address = fields[2]
                amount = int(fields[3])
                yield(address, (amount, 1))

            else:                            #SCAMS
                fields = line.split(',')
                sc_category = fields[0]
                sc_addresses = fields[1:]

                for address in sc_addresses:
                    yield(address, (sc_category,2))
        except:
            pass

    def reducer1(self, address, values):

            amount = 0
            category = None

            for value in values:
                if value[1] == 1:
                    amount += value[0]
                elif value[1] == 2:
                    category = value[0]

            if category != None:
                yield(category, amount)


    def reducer2(self, category, values):
            yield(category, sum(values))

    def steps(self):
        return [MRStep(mapper=self.mapper1,
                        reducer=self.reducer1),
                MRStep(reducer=self.reducer2)]

if __name__ == '__main__':
    scam_analysis.run()
