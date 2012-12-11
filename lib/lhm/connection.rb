module Lhm
  class Connection
    def self.new(adapter)
      if defined?(DataMapper) && DataMapper::Adapters::AbstractAdapter === adapter
        DataMapperConnection.new(adapter)
      elsif defined?(ActiveRecord) && ActiveRecord::ConnectionAdapters::AbstractAdapter === adapter
        ActiveRecordConnection.new(adapter)
      end
    end

    class DataMapperConnection
      def initialize(adapter)
        @adapter = adapter
        @database_name = adapter.options['database']
      end

      def current_database
        @database_name
      end

      def select_all(sql)
      end

      def select_row(sql)
      end

      def select_value(sql)
      end

      def select_all(sql)
      end

      def execute(sql)
      end

      def update(sql)
      end

      def table_exists?(table_name)
      end
    end

    class ActiveRecordConnection
    end
  end
end
