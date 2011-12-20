
module RzmqBrokers

  # Wraps a regular Array. Makes sure that all inserts are done in
  # sorted order (inserted objects must be Comparable).
  #
  # Searches take advantage of the sorted order to quickly find items.
  #
  class SortedArray
    include Enumerable

    def initialize
      @array = []
    end

    def each(&blk)
      @array.each { |value| yield(value) }
    end

    def delete(value)
      i = index(value) - 1
      @array.delete_at(i) if value == @array.at(i)
    end

    def delete_at(i)
      @array.delete_at(i)
    end

    def insert(value)
      @array.insert(index(value), value)
    end
    alias :<< :insert
    alias :push :insert
    alias :unshift :insert

    def direct_insert(i, value)
      @array.insert(i, value)
    end

    def at(i)
      @array.at(i)
    end
    alias :[] :at

    def size() @array.size; end

    # Original Ruby source Posted by Sergey Chernov (sergeych) on 2010-05-13 20:23
    # http://www.ruby-forum.com/topic/134477
    #
    # binary search; assumes underlying array is already sorted
    #
    def index(value)
      l, r = 0, @array.size - 1

      # if r == 0, then the array is empty; don't iterate
      if r > 0
        while l <= r
          m = (r + l) / 2

          # similar to (value < @array.at(m)) but works with anything Comparable
          if -1 == (value <=> @array.at(m))
            r = m - 1
          else
            l = m + 1
          end
        end
      end
      l
    end

    def inspect
      @array.inspect
    end

    def include?(value)
      # When the value already exists in the array, the call to #index will find
      # it and return an index just to the right of it; that is, duplicates are
      # added to the right. So, to check if this is a dupe we need to subtract
      # 1 from the given index and compare the values.
      i = index(value) - 1
      @array.at(i) == value
    end

  end # SortedArray

end # RzmqBrokers
