# Copyright (c) 2011, SoundCloud Ltd., Rany Keddo, Tobias Bielohlawek, Tobias
# Schmidt

require 'lhm/command'
require 'lhm/sql_helper'

module Lhm
  class Chunker
    include Command
    include SqlHelper

    attr_reader :connection

    # Copy from origin to destination in chunks of size `stride`. Sleeps for
    # `throttle` milliseconds between each stride.
    def initialize(migration, connection = nil, options = {})
      @migration = migration
      @connection = connection
      @stride = options[:stride] || 40_000
      @throttle = options[:throttle] || 100
      @chunker_column = options[:chunker_column] || migration.origin.pk
      @start = options[:start] || select_start
      @limit = options[:limit] || select_limit
    end

    # Copies chunks of size `stride`, starting from `start` up to id `limit`.
    def up_to(&block)
      1.upto(traversable_chunks_size) do |n|
        yield(bottom(n), top(n))
      end
    end

    def traversable_chunks_size
      @limit && @start ? ((@limit - @start + 1) / @stride.to_f).ceil : 0
    end

    def bottom(chunk)
      value = (chunk - 1) * @stride 
      value == 0 ? 0 : value - 1
    end

    def top(chunk)
      [chunk * @stride + @start - 1, @limit].min
    end

    def copy(lowest, highest)
      "insert ignore into `#{ destination_name }` (#{ columns }) " +
      "select #{ columns } from `#{ origin_name }` " +
      "order by  `#{@chunker_column}` limit #{ lowest },#{ highest }"
    end

    def select_start
      start = connection.select_value("select min(#{@chunker_column}) from #{ origin_name }")
      start ? 0 : nil
    end

    def select_limit
      limit = connection.select_value("select count(#{@chunker_column}) from #{ origin_name }")
      limit ? limit.to_i : nil
    end

    def throttle_seconds
      @throttle / 1000.0
    end

  private

   
    def destination_name
      @migration.destination.name
    end

    def origin_name
      @migration.origin.name
    end

    def columns
      @columns ||= @migration.intersection.joined
    end

    def validate
      if @start && @limit && @start > @limit
        error("impossible chunk options (limit must be greater than start)")
      end
    end

    def execute
      up_to do |lowest, highest|
        affected_rows = update(copy(lowest, highest))

        if affected_rows > 0
          sleep(throttle_seconds)
        end

        print "."
      end
      print "\n"
    end
  end
end





