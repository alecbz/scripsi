# Scripsi

A flexible text-searching library built on top of redis.

## Sorted suffix indexing

Sorted suffix indexing allows you to search for any substring within a set of documents. First, index a collection of documents and associated ids:

    require 'scripsi'
    Scripsi.connect  # connect to a running redis server

    ssi = Scripsi::SortedSuffixIndexer.new "myindexer"
    ssi.index(1,"Epistulam ad te scripsi.")
    ssi.index(2,"I've written you a letter.")
    ssi.index(3,"Quisnam Tusculo espistulam me misit?")
    ssi.index(4,"Who in Tusculum would've sent me a letter?")

You can then search for any substring, and the indexer will return the ids of the documents where that substring appears.

    ssi = Scripsi.indexer "myindexer"
    ssi.search("te")        # => [1,2,4]
    ssi.search("Tuscul")    # => [3,4]
    ssi.search("Tusculu")   # => [4]
    ssi.search("you a le")  # => [2]

You can also retrive the stored documents efficiently:

    ssi.documents  # lazy list of documents
    ssi.documents[3]  # document with id string
