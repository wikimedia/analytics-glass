from tagging import Tag

bots = Tag(entity='user', predicate='bot')

if 16737026 in bots:
    print "16737026 is a bot."

print "total number of bots is %d" % len(bots)
