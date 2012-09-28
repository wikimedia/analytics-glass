import redis
import array


r = redis.Redis()


def unpack(int):
    return [int >> offset & 1 for offset in range(7, -1, -1)]


class Tag(object):

    key_template = 'tag:%(entity)s:%(predicate)s'

    def __init__(self, entity, predicate):
        self.key = Tag.key_template % locals()

    def tag(self, item):
        self.__setitem__(item, 1)

    def untag(self, item):
        self.__setitem__(item, 0)

    def to_a(self):
        return array.array('B', r.get(self.key))

    def __getitem__(self, item):
        return r.getbit(self.key, item) == 1

    __contains__ = __getitem__ 

    def __setitem__(self, item, value):
        r.setbit(self.key, item, int(value))

    def __len__(self):
        return r.bitcount(self.key)

    def __iter__(self):
        i = 0
        for n in self.to_a():
            for bit in unpack(n):
                if bit == 1:
                    yield i
                i = i + 1
