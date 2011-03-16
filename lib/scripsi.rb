require 'redis'
require 'benchmark'
require 'strscan'
require 'set'

COMMON_WORDS = %w{a an the of if in}

module Scripsi
  # connect to a redis server
  def self.connect(options = {})
    @@redis = Redis.new(options)
  end

  # reference to the redis object
  def self.redis
    @@redis
  end

  # the size of the partitions to break words up by
  def self.partition_size
    10
  end

  # generate a 'score' for a string
  # used for storing it in a sorted set
  #
  # This method effectively turns a string into a base 27 floating point number,
  # where 0 corresponds to no letter, 1 to A, 2 to B, etc.
  #
  # @param [String] str the string we are computing a score for
  # @return [Array] the string's score(s), divided into chunks each representing 10 characters
  def self.score(str, max_length = 30)
    str = str.downcase
    scrs = []
    str[0,max_length].split('').each_slice(partition_size) do |s|
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
  def self.find(id)
    indexer(id)
  end

  # get the indexer with the given id
  def self.indexer(id)
    type = Scripsi.redis.hget "scripsi:used", id.to_s
    if type == "ssi"
      Indexer.build(id)
    end
  end

  def self.write_number(n,width=8)
    str = ""
    n = n.to_i
    until n == 0
      str << (n%256).chr
      n /= 256
    end
    str.ljust(width,"\0")
  end

  def self.read_number(str,width=8)
    n, factor = 0, 1
    str.each_byte do |b|
      n += b*factor
      factor *= 256
    end
    n
  end

  class Indexer
    # initialize this indexer
    #
    # @param [Object] id the id to reference this indexer by
    # @param [Boolean] check internally used flag.
    #   LEAVE AS true. disabling the flag might result in overwritting an indexer
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
      @index_key = "scripsi:#{@id}:index"
      @document_key = "scripsi:#{@id}:document"
      @id_info_key = "scripsi:#{@id}:id_info"
      @index_flat_key = "scripsi:#{@id}:index_flat"
      @search_length = 30
    end

    # completely destroy this indexer
    def destroy
      i = 0
      loop do
        d = Scripsi.redis.del "#{@index_key}:#{i}"
        break unless d == 1
        i += 1
      end
      Scripsi.redis.del @document_key
      #Scripsi.redis.del @documents_key
      Scripsi.redis.del @id_info_key 
      Scripsi.redis.hdel "scripsi:used", @id
    end

    # adds a document to this indexer
    #
    # @param [String,Number] id a string representing the id of this document
    #   (numbers also work, they will be converted with to_s)
    # @param [String] str the text of the document
    # @return [Boolean] returns true if the document was successfully indexed
    def index(id,str)
      id = id.to_s
      #return false if Scripsi.redis.hexists @documents_key, id
      return false if Scripsi.redis.zscore @id_info_key, id
      offset = Scripsi.redis.strlen @document_key
      sfxs = suffixes(str).sort_by{|s,i| s}
      sfxs.each do |suffix,i|
        Scripsi.score(suffix,@search_length).each_with_index do |scr,j|
          Scripsi.redis.zadd "#{@index_key}:#{j}", scr, i+offset
        end
      end
      Scripsi.redis.append @document_key, str
      Scripsi.redis.zadd @id_info_key, offset + str.size, id
    end

    # flattens the docoument index
    # this reduces memory usage significantly, but will also slightly increase access times
    #
    # flattening the index will operate on the entire set of searchable data. this will be a costly operation if you have a lot of data indexed
    def flatten_index
      #new_width = (Math.log(Scripsi.redis.strlen(@document_key),2)/8).ceil
      indxs = Scripsi.redis.zrange "#{@index_key}:0", 0, -1
      if Scripsi.redis.strlen(@index_flat_key) == 0
        str = indxs.map{|indx|Scripsi.write_number(indx)}.join
        Scripsi.redis.set @index_flat_key, str
      else
        insertions = {}
        current = Scripsi.redis.get @index_flat_key
        size = Scripsi.redis.strlen(@index_flat_key)/8
        slot = 0
        indxs.each_with_index do |indx,i|
          if slot >= size
            insertions[slot] = indx
            next
          end
          indx = indx.to_i
          puts "indx: #{indx}, slot: #{slot}, size: #{size}"
          data = current[i..i*8]
          n = Scripsi.read_number(data)
          word = string_at_index(n)
          cmp = compare_with_index(word,i)
          if cmp < 1
            slot += 1
            redo
          else
            insertions[slot] = indx
          end
        end
        # TODO: insert the stuff
        p insertions
      end

#      i = 0
#      loop do
#        d = Scripsi.redis.del "#{@index_key}:#{i}"
#        break unless d == 1
#        i += 1
#      end
    end

    # a lazy list of documents associated with a Indexer
    class Documents
      def initialize(doc_key, info_key)
        @doc_key = doc_key
        #@endpoints_key = endpoints_key
        @id_info_key = info_key
      end

      # get the document with the given id
      #
      # @param [String] id the id of the document
      # @return [String] the original text of the document
      def [](id)
        a, b = endpoints(id.to_s)
        if a and b
          Scripsi.redis.getrange @doc_key, a.to_i, b.to_i
        end
      end

      # get the offset of the document with the given id in the main document string
      #   (used internally)
      #
      # @param (see #[])
      # @return [Integer] the offset of the document
      def offset_of(id)
        a,b = endpoints(id.to_s)
        if a and b
          a.to_i
        end
      end

      private 

      def endpoints(id)
        endpoint = Scripsi.redis.zscore @id_info_key, id
        return nil unless endpoint
        a,b = "(#{endpoint}", "-inf"
        prev_id = Scripsi.redis.client.call(:zrevrangebyscore,@id_info_key, a, b,"LIMIT",0,1).first
        start = Scripsi.redis.zscore @id_info_key, prev_id
        [start.to_i,endpoint.to_i - 1]
      end

    end

    # retrive a lazy list of this indexer's documents
    #
    # @return [Documents] lazy list of documents
    def documents
      Documents.new(@document_key,@id_info_key)
    end

    # searches for documents containing the substring term
    #
    # @param [String] term the substring to search for
    # @return [Array] an array of document ids that term appears in
    def search(term)
      set = base_search(term)
      set.map{|i| get_id(i.to_i) }.uniq
    end

    # class representing a search match
    class Match
      def initialize(index,term,indexer)
        @index = index.to_i
        @term = term
        @indexer = indexer
      end

      # get the document id of this match
      def doc
        @doc ||= @indexer.get_id(@index)
        @doc
      end

      # get an array of endpoints representing the start and end indexes
      #   of the match in the original document string
      def endpoints
        unless @endpoints
          offset = @indexer.documents.offset_of(@doc)
          a,b = nil
          if offset
            a = @index - offset
            b = a + @term.length
          end
          @endpoints = [a,b]
        end
        @endpoints
      end

      # get the index of where the match starts
      #   in the original document string
      def start_index
        endpoints[0]
      end

      # get the index of where the match ends
      #   in the original document string
      def end_index
        endpoints[1]
      end

      def inspect
        "#<Scripsi::Indexer::Match>"
      end
    end

    # gets document ids and the matched indexes
    # of documents containing the term
    #
    # @param (see #search)
    # @return [Array] an array of MatchData objects,
    #   containing the document id and the start/end indexes for each match
    def matches(term)
      set = base_search(term)
      set.map do |i|
        #        doc_id = get_id(i.to_i)
        #        offset = documents.offset_of(doc_id)
        #        a,b = nil
        #        if offset
        #          a = i.to_i - offset
        #          b = a + term.length
        #        end
        #        MatchData.new(doc_id,a,b)
        Match.new(i,term,self)
      end
    end

    # creates an indexer with the given id WITHOUT CHECKING
    # this method is used internally - calling it yourself may result in deleting an indexer, unless you know the id you're using is valid
    def self.build(id)
      new(id,false)
    end

    def inspect
      "#<Scripsi::Indexer id=#{@id}>"
    end

    # private

    def base_search(term)
      return [] if term.empty? # bug with searching for empty strings
      term, length = term.downcase, term.length
      set = nil

      #search the live index
      Scripsi.score(term, @search_length).each_with_index do |scr,i|
        a,b = scr.to_s, "#{scr+1.0/(27**length)}"
        b = "(" + b unless a == b
        ids = Scripsi.redis.zrangebyscore("#{@index_key}:#{i}",a,b).map &:to_i
        set = set ? set & Set.new(ids) : Set.new(ids)
        length -= Scripsi.partition_size
      end
      STDERR.puts "set: #{set.to_a}"

      #return set
      #search the flat index
      lo,hi = 0, Scripsi.redis.strlen(@index_flat_key)/8
      until lo > hi
        m = (lo+hi)/2
        #puts "m: #{m}"
        a = m*8
        b = a+7
        #puts "a,b: #{a},#{b}"
        data = Scripsi.redis.getrange(@index_flat_key,a,b)
        break unless data
        index = Scripsi.read_number(data)
        #puts "index: #{index}"
        cmp = compare_with_index(term,index)
        #puts "cmp: #{cmp}"
        if cmp < 0
          hi = m-1
        elsif cmp > 0
          lo = m+1
        else
          s = Set.new [index]
          ##puts "found index #{index} in flat index"
          loop do
            a += 8
            b += 8
            #puts "a,b: #{a},#{b}"
            data = Scripsi.redis.getrange @index_flat_key, a, b
            break unless data
            index = Scripsi.read_number data
            if compare_with_index(term,index) == 0
              s.add(index)
            else
              break
            end
          end
          set = set ? (set | s) : s
          break
        end
      end
      #      if lo == hi
      #        #puts "found index #{lo} in flat index"
      #      end

      ##puts "returning #{set.to_a}"
      set
    end

    def suffixes(str)
      str = str.downcase
      scan = StringScanner.new str
      res = []
      until scan.eos?
        scan.skip(/\s+/)
        res << [str[scan.pos..-1],scan.pos]
        word = scan.scan(/[^\s]+/)
        res.pop if COMMON_WORDS.include? word
      end
      res
    end

    def document_index(index)
      doc_index = Scripsi.redis.zrange(@index_key, index, index).first.to_i
    end

    def string_at_index(index,len=30)
      Scripsi.redis.getrange @document_key, index, index+len-1
    end

    def compare_with_index(str,index)
      #puts "-"*5 + "compare_with_index(#{str},#{index})" + "-"*5
      len = [30,str.length].min
      str <=> string_at_index(index,len).downcase
      #puts "#{str.inspect} <=> #{s.downcase.inspect}  ->   #{str <=> s.downcase}"
    end

    def print_flat_index
      data = Scripsi.redis.get @index_flat_key
      data.split('').each_slice(8) do |slice|
        p Scripsi.read_number(slice.join)
      end
    end

    def get_id(index)
      Scripsi.redis.zrangebyscore(@id_info_key, "(#{index}", "+inf", :limit => [0,1]).first
      #      puts "-"*20, "get_id", "-"*20
      #      last = nil
      #      puts "comptuing last:"
      #      puts Benchmark.measure{ last = Scripsi.redis.strlen @document_key }*1000
      #      getting_to_id = Benchmark::Tms.new
      #      reads = 0
      #      (doc_index..last).each do |i|
      #        char = nil
      #        getting_to_id +=  Benchmark.measure{char = Scripsi.redis.getrange(@document_key, i, i)}*1000
      #        reads += 1
      #        if char == "\0"
      #          puts "getting to id (took #{reads} reads, avg read: #{getting_to_id.real/reads}):"
      #          puts getting_to_id
      #          puts "reading id:"
      #          id = ""
      #          puts Benchmark.measure {
      #            offset = 1
      #            loop do
      #              next_char = Scripsi.redis.getrange(@document_key,i+offset,i+offset)
      #              if next_char == "\0"
      #                break
      #              else
      #                id << next_char
      #                offset += 1
      #              end
      #            end
      #          }*1000
      #          return id
      #        end
      #      end
      #      raise "index is corrupt"
    end
  end

end
