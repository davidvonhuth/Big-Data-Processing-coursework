from mrjob.job import MRJob
from mrjob.step import MRStep

class top_ten_popularServices(MRJob):
    def mapper1(self, _,line):
        try:
            if (len(line.split(',')) == 5): #CONTRACTS
                fields = line.split(',')
                c_address = fields[0]
                yield(c_address, (None,1))

            elif(len(line.split(',')) == 7): #TRANSACTIONS,   
                fields = line.split(',')
                address = fields[2]
                amount = int(fields[3])
                yield(address, (amount, 2))
        except:
            pass

    def reducer1(self, address, values):
            amount = 0
            c_address = None

            for value in values:
                if value[1] == 1:
                    c_address = address
                elif value[1] == 2:
                    amount += value[0]

            if c_address != None:
                yield(None, (c_address, amount))
    
    def reducer2(self, _, values):
            sorted_values = sorted(values, reverse=True, key=lambda x: x[1])
            top_ten = sorted_values[0:10]

            for contract in top_ten:
                yield("{} - {}".format(contract[0], contract[1]), None)

    def steps(self):
        return [MRStep(mapper=self.mapper1,
                        reducer=self.reducer1),
                MRStep(reducer=self.reducer2)]

if __name__ == '__main__':
    top_ten_popularServices.run()
    # print("\n------------ %s seconds -----------" % np.round((time.time() - start_time),3))


# def combiner1(self, address, values):
#     amount = 0
#     contract = None

#     for value in values:
#         if value[1] == 1:
#             contract = address
#         elif value[1] == 2:
#             amount += value[0]

#     if contract != None:
#         yield(contract, amount)






# def combiner2(self, _, values):
#     sorted_values = sorted(values, reverse=True, key=lambda x: x[1])
#     top_ten = sorted_values[:10]

#     for contract in top_ten:
#         yield(None, contract)
