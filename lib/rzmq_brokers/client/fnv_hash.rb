
module RzmqBrokers
  module Client

    # Ruby version (c) 2011 by Evan Phoenix
    #
    class FNV
      OffsetBasis = 2166136261
      HashPrime = 16777619
      Mask = 0x7fffffff
      Width = 31

      def self.hash_string(str)
        hv = OffsetBasis

        str.each_byte do |byte|
          hv = ((hv ^ byte) * HashPrime) & Mask
        end

        (hv >> Width) ^ (hv & Mask)
      end
    end

  end
end

if $0 == __FILE__
  p FNV.hash_string("foo")
  p FNV.hash_string("bar")
end
