# library/module for handy filters/maps

# I'm certain theres something that already exists for this in python, there is in Lua
# and Ruby, but I couldn't find it in the sequence/list doco,
# But this maps an element to a tuple containing the element and the index in the iteration.
# UPDATE: found it, the 'enumerate()' method was added for exactly this purpose in Python 2.3
# so iterUtils.ElementIndexMap(list) is equivalent to enumerate(list).
class ElementIndexMap:
    def __init__(self, collection, initialIndex=0):
        self.__collection = collection
        self.__index = initialIndex
        return

    def __iter__(self):
        return self

    def next(self):
        index = self.__index
        self.__index += 1

        return (self.__collection.next(), index)

# this takes a sequence of dictionaries with identical key sets sort of like a 'star' configuration and iterates over the
# all the keys and groups the values together.
# technically it terates over the key set of the first one in the collection and then does lookups on the rest,
# so the other maps can contain additional keys, those values justr wont get iterated over.
# you can optionally give it a list of key items if you want to specify an orderd in which the keys are looked up.
class EnumerateStarMaps:
    def __init__(self, dictionaries, keys=None):
        self.__dictionaries = dictionaries
        # dictionaries.__iter__()
        if keys == None:
            self.__keyGenerator = dictionaries[0].iterkeys()
        else:
            self.__keyGenerator = iter(keys)
             
        return

    def __iter__(self):
        return self

    def next(self):
        key = self.__keyGenerator.next()

        result = [key]

        for dictionary in self.__dictionaries:
            result.append(dictionary[key])

        return tuple(result)

# for iterating over dictionaries, you give it a key that points to a Comparable data type and it will
# check each dictionary you call it with and keep a running tally of the minimum and maximum values encountered so far.
class MinMaxBounds:
    def __init__(self, key):
        self.__min = None
        self.__max = None

        self.__key = key
        return

    def __call__(self, inputDictionary):
        if self.__min == None or self.__min > inputDictionary[self.__key]:
            self.__min = inputDictionary[self.__key]

        if self.__max == None or self.__max < inputDictionary[self.__key]:
            self.__max = inputDictionary[self.__key]

        return inputDictionary

    def getMin(self):
        return self.__min

    def getMax(self):
        return self.__max

    def getKey(self):
        return self.__key

class ElementWithKey:
    def __init__(self, key):
        self.__key = key
        return

    def __call__(self, element):
        return element[self.__key]

# you give it a generator and it will just keep taking values until the generator is spent,
# good for "foreach" scenarios where you want to map a function over the contents of a list but dont want to do anything with the result, like put it in a list or something.
def takeAll(generator):
    try:
        while True:
            generator.next()
    except StopIteration:
        pass

    return
