require 'redis'
require 'set'

module Scripsi
  # connect to a redis server
  def self.connect(options = {})
    @@redis = Redis.new(options)
  end

  def self.redis
    @@redis
  end

  @@partition_size = 10

  def self.partition_size
    @@partition_size
  end

  # generate a 'score' for a string
  # used for storing it in a sorted set
  #
  # This method effectively turns a string into a base 27 floating point number,
  # where 0 corresponds to no letter, 1 to A, 2 to B, etc.
  #
  # @param [String] str the string we are computing a score for
  # @return [Number] the string's score
  def self.score(str)
    str = str.downcase
    scrs = []
    str.split('').each_slice(partition_size) do |s|
      mult = 1.0
      scr = 0.0
      s.each do |char|
        mult /= 27
        scr += (char.ord-'a'.ord+1)*mult if ('a'..'z').include? char
      end
      scrs << scr
    end
    scrs
  end

  # get the indexer with the given id
  def self.indexer(id)
    type = Scripsi.redis.hget "scripsi:used", id.to_s
    if type == "ssi"
      SortedSuffixIndexer.build(id)
    end
  end

  # (see #indexer)
  def self.find(id)
    indexer(id)
  end

  class SortedSuffixIndexer
    def initialize(id=nil,check=true)
      if check
        if id and Scripsi.redis.hexists "scripsi:used", id.to_s
          raise "id '#{id}' in use"
        end
        @id = id ? id.to_s : Scripsi.redis.incr("scripsi:next_id")
        Scripsi.redis.hset "scripsi:used", @id, "ssi"
      else
        @id = id
      end
      @index_key = "scripsi:index:#{@id}"
      @document_key = "scripsi:document:#{@id}"
      @documents_key = "scripsi:documents:#{@id}"
      @search_length = 30
    end

    # adds a document to this indexer
    #
    # @param [Integer] id a number representing the id of this document
    # @param [String] str the text of the document
    # @return [Boolean] returns true if the document was successfully indexed
    def index(id,str)
      id = id.to_s
      return false if Scripsi.redis.hexists @documents_key, id
      offset = Scripsi.redis.strlen @document_key
      sfxs = suffixes(str).sort_by{|s,i| s}
      sfxs.each do |suffix,i|
        Scripsi.score(suffix).each_with_index do |scr,j|
          Scripsi.redis.zadd "#{@index_key}:#{j}", scr, i+offset
        end
      end
      doc = str + "\0#{id}\0"
      Scripsi.redis.append @document_key, doc
      endpoints = Marshal.dump([offset, offset + str.size - 1])
      Scripsi.redis.hset @documents_key, id, endpoints
    end

    # a lazy list of documents associated with a SortedSuffixIndexer
    class Documents
      def initialize(doc_key, endpoints_key)
        @doc_key = doc_key
        @endpoints_key = endpoints_key
      end

      def [](id)
        a, b = endpoints(id)
        if a and b
          Scripsi.redis.getrange @doc_key, a.to_i, b.to_i
        end
      end

      def offset_of(id)
        a,b = endpoints(id)
        if a and b
          a.to_i
        end
      end

      private 

      def endpoints(id)
        endpoints = Scripsi.redis.hget(@endpoints_key, id)
        if endpoints
          Marshal.load(endpoints)
        end
      end

    end

    # retrive the document with the given id
    def documents
      Documents.new(@document_key,@documents_key)
    end

    # searches for documents containing the substring term
    #
    # @param [String] term the substring to search for
    # @return [Array] an array of document ids that term appears in
    def search(term)
      set = base_search(term)
      set.map{|i| read_to_id(i.to_i) }
    end

    MatchData = Struct.new(:doc, :start, :end)

    # gets document ids and the matched indexes
    # of documents containing the term
    #
    # @param (see #search)
    # @return [Array] an array of MatchData structs,
    #   containing the id of the matched document and the indexes of where the match begins and ends
    def matches(term)
      set = base_search(term)
      set.map do |i|
        doc_id = read_to_id(i.to_i)
        offset = documents.offset_of(doc_id)
        a,b = nil
        if offset
          a = i.to_i - offset
          b = a + term.length
        end
        MatchData.new(doc_id,a,b)
      end
    end

    # creates an indexer with the given id WITHOUT CHECKING
    # this method is used internally - calling it yourself may result in deleting an indexer, unless you know the id you're using is valid
    def self.build(id)
      new(id,false)
    end

    def inspect
      "#<Scripsi::SortedSuffixIndexer id=#{@id}>"
    end

    private

    def base_search(term)
      term, length = term.downcase, term.length
      set = nil
      Scripsi.score(term).each_with_index do |scr,i|
        a,b = scr.to_s, "#{scr+1.0/(27**length)}"
        b = "(" + b unless a == b
        ids = Scripsi.redis.zrangebyscore("#{@index_key}:#{i}",a,b)
        set = set ? set & Set.new(ids) : Set.new(ids)
        length -= Scripsi.partition_size
      end
      set
    end

    def suffixes(str)
      str = str.downcase
      (0...str.length).map {|i| [str[i..-1],i] }
    end

    def document_index(index)
      doc_index = Scripsi.redis.zrange(@index_key, index, index).first.to_i
    end

    def compare_with_index(str,doc_index)
      str.split('').each_with_index do |char,offset|
        s = Scripsi.redis.getrange @document_key, doc_index+offset, doc_index+offset
        STDERR.puts "comparing #{char} and #{s.downcase}"
        comp = char <=> s.downcase
        return comp unless comp == 0
      end
      0
    end

    def read_to_id(doc_index)
      last = Scripsi.redis.strlen @document_key
      (doc_index..last).each do |i|
        char = Scripsi.redis.getrange(@document_key, i, i)
        if char == "\0"
          id = ""
          offset = 1
          loop do
            next_char = Scripsi.redis.getrange(@document_key,i+offset,i+offset)
            if next_char == "\0"
              break
            else
              id << next_char
              offset += 1
            end
          end
          return id
        end
      end
      raise "index is corrupt"
    end
  end

end
