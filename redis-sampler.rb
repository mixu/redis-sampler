#!/usr/bin/env ruby

# Copyright (c) 2011, Salvatore Sanfilippo
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of Redis nor the names of its contributors may be
#       used to endorse or promote products derived from this software without
#       specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
# TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
# PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
# CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
# EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
# PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
# OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
# WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
# OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
# ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

require 'rubygems'
require 'redis'

class RedisSampler
    def initialize(r,samplesize)
        @redis = r
        @samplesize = samplesize
        @types = {}
        @expires = {}
        @zset_card = {}
        @zset_elesize = {}
        @list_len = {}
        @list_elesize = {}
        @hash_len = {}
        @hash_fsize = {}
        @hash_vsize = {}
        @set_card = {}
        @set_elesize = {}
        @string_elesize = {}
    end

    def incr_freq_table(hash, prefix, item)
        if !hash[prefix]
            hash[prefix] = {}
        end
        hash[prefix][item] = 0 if !hash[prefix][item]
        hash[prefix][item] += 1
    end

    def sample
        @samplesize.times {
            k = @redis.randomkey
            p = @redis.pipelined {
                @redis.type(k)
                @redis.ttl(k)
            }
            # from beginning to first ":"
            prefix = k.split(':', 2)[0]
            t = p[0]
            x = p[1]
            x = 'unknown' if x.to_i == -1
            incr_freq_table(@types, prefix, t)
            incr_freq_table(@expires, prefix, x)
            case t
            when 'zset'
                p = @redis.pipelined {
                    @redis.zcard(k)
                    @redis.zrange(k,0,0)
                }
                card = p[0]
                ele = p[1][0]
                incr_freq_table(@zset_card, prefix, card) if card != 0
                incr_freq_table(@zset_elesize, prefix, ele.length) if ele
            when 'set'
                p = @redis.pipelined {
                    @redis.scard(k)
                    @redis.srandmember(k)
                }
                card = p[0]
                ele = p[1]
                incr_freq_table(@set_card, prefix, card) if card != 0
                incr_freq_table(@set_elesize, prefix, ele.length) if ele
            when 'list'
                p = @redis.pipelined {
                    @redis.llen(k)
                    @redis.lrange(k,0,0)
                }
                len = p[0]
                ele = p[1][0]
                incr_freq_table(@list_len, prefix, len) if len != 0
                incr_freq_table(@list_elesize, prefix, ele.length) if ele
            when 'hash'
                l = @redis.hlen(k)
                incr_freq_table(@hash_len, prefix, l) if l != 0
                if l >= 1
                    field = @redis.hkeys(k)[0]
                    if field
                        incr_freq_table(@hash_fsize, prefix, field.length)
                        val = @redis.hget(k,field)
                        incr_freq_table(@hash_vsize, prefix, val.length) if val
                    end
                else
                    incr_freq_table(@hash_fsize, prefix, 'unknown')
                    incr_freq_table(@hash_vsize, prefix, 'unknown')
                end
            when 'string'
                incr_freq_table(@string_elesize, prefix, @redis.strlen(k))
            end
        }
    end

    def perc(val,tot)
        sprintf("%.2f%%",val*100/tot.to_f)
    end

    def render_each_freq_table(title,hash)
        puts "\n#{title.upcase}\n#{"="*title.length}\n" if title

        hash.each{|prefix, h|
            puts "\n# for #{prefix}"
            render_freq_table(nil, h)
        }
   end

    def render_freq_table(title,hash)
        h = hash.sort{|a,b| b[1]<=>a[1]}
        i = 0
        tot = 0
        h.each{|k,v| tot += v.to_i}
        partial = 0
        h.each{|k,v|
            partial += v
            s = " #{k}: #{v} (#{perc v,tot})"
            s += " " * (25 - s.length) if s.length < 25
            print s
            i += 1
            puts "" if i % 3 == 0
            break if i >= 21 and v/@samplesize.to_f < 0.005
        }
        puts "" if i % 3 != 0
        if i != h.length
            puts "(suppressed #{h.length-i} items with perc < 0.5% for a total of #{perc tot-partial,tot})"
        end
    end

    def num_to_pow(n)
        # Convert the number into the smallest power of two that
        # is equal or greater of the number.
        p = 1
        p *= 2 while n > p
        p
    end

    def render_power_table(hash)
        hash.each{|prefix, hash|
            puts "\n# for #{prefix}"
            puts "\nPowers of two distribution: (NOTE <= p means: p/2 < x <= p)"
            pow = {}
            hash.each{|k,v|
                next if k == 'unknown'
                p = "<= #{num_to_pow(k)}"
                pow[p] = 0 if !pow[p]
                pow[p] += v
            }
            render_freq_table(nil,pow)
        }
    end

    def compute_avg(hash)
        # Compute average
        min = max = nil
        avg = 0
        items = 0
        hash.each{|k,v|
            next if k == 'unknown'
            avg += k*v
            items += v
            min = k if !min or min > k
            max = k if !max or max < k
        }
        if items.to_i == 0
            return {:avg => 0, :stddev => 0, :min => 0, :max => 0}
        end
        avg /= items.to_f
        # Compute standard deviation
        stddev = 0
        hash.each{|k,v|
            next if k == 'unknown'
            stddev += ((k-avg)**2)*v
        }
        stddev = Math.sqrt(stddev/items.to_f)
        return {:avg => avg, :stddev => stddev, :min => min, :max => max}
    end

    def render_avg(hash)
        hash.each{|prefix, hash|
            puts "\n# for #{prefix}"
            data = compute_avg(hash)
            printf " Average: %.2f Standard Deviation: %.2f",data[:avg],data[:stddev]
            puts ""
            puts " Min: #{data[:min]} Max: #{data[:max]}"
        }
    end

    def stats
        render_each_freq_table("Types",@types)
        render_each_freq_table("Expires",@expires)
        render_avg(@expires)
        render_power_table(@expires)
        puts "\nNote: 'unknown' expire means keys with no expire"
        if @string_elesize.length != 0
            render_each_freq_table("Strings, size of values",@string_elesize)
            render_avg(@string_elesize)
            render_power_table(@string_elesize)
        end
        if @list_len.length != 0
            render_each_freq_table("Lists, number of elements",@list_len)
            render_avg(@list_len)
            render_power_table(@list_len)
            render_each_freq_table("Lists, size of elements",@list_elesize)
            render_avg(@list_elesize)
            render_power_table(@list_elesize)
        end
        if @set_card.length != 0
            render_each_freq_table("Sets, number of elements",@zset_card)
            render_avg(@set_card)
            render_power_table(@set_card)
            render_each_freq_table("Sets, size of elements",@set_elesize)
            render_avg(@set_elesize)
            render_power_table(@set_elesize)
        end
        if @zset_card.length != 0
            render_each_freq_table("Sorted sets, number of elements",@zset_card)
            render_avg(@zset_card)
            render_power_table(@zset_card)
            render_each_freq_table("Sorted sets, size of elements",@zset_elesize)
            render_avg(@zset_elesize)
            render_power_table(@zset_elesize)
        end
        if @hash_len.length != 0
            render_each_freq_table("Hashes, number of fields",@hash_len)
            render_avg(@hash_len)
            render_power_table(@hash_len)
            render_each_freq_table("Hashes, size of fields",@hash_fsize)
            render_avg(@hash_fsize)
            render_power_table(@hash_fsize)
            render_each_freq_table("Hashes, size of values",@hash_vsize)
            render_avg(@hash_vsize)
            render_power_table(@hash_vsize)
        end
        puts ""
    end
end

if ARGV.length != 4
    puts "Usage: redis-sampler.rb <host> <port> <dbnum> <sample_size>"
    exit 1
end

redis = Redis.new(:host => ARGV[0], :port => ARGV[1].to_i, :db => ARGV[2].to_i)
sampler = RedisSampler.new(redis,ARGV[3].to_i)
puts "Sampling #{ARGV[0]}:#{ARGV[1]} DB:#{ARGV[2]} with #{ARGV[3]} RANDOMKEYS"
sampler.sample
sampler.stats
