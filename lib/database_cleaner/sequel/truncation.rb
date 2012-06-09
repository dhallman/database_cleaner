require "database_cleaner/generic/truncation"
require 'database_cleaner/sequel/base'

module DatabaseCleaner
  module Sequel
    class Truncation
      include ::DatabaseCleaner::Sequel::Base
      include ::DatabaseCleaner::Generic::Truncation
  
      def clean
        case db.database_type
        when :postgres;
          # PostgreSQL requires all tables with FKs to be truncates in the same command, or have the CASCADE keyword
          # appended. Bulk truncation without CASCADE is:
          # * Safer. Tables outside of tables_to_truncate won't be affected.
          # * Faster. Less roundtrips to the db.
          unless (tables= tables_to_truncate(db)).empty?
            all_tables= tables.map{|t| %["#{t}"]}.join ','
            db.run "TRUNCATE TABLE #{all_tables};"
          end
        when :mysql;
          # Make sure to turn off foreign key checks, or truncate command fails
          db.run "set FOREIGN_KEY_CHECKS=0;"
          # Truncate each table normally
          each_table do |db, table|
            db[table].truncate
          end
          db.run "set FOREIGN_KEY_CHECKS=1;"
        else
          # Truncate each table normally
          each_table do |db, table|
            db[table].truncate
          end
        end
      end
  
      def each_table
        tables_to_truncate(db).each do |table|
          yield db, table
        end
      end

      private
  
      def tables_to_truncate(db)
        (@only || db.tables) - @tables_to_exclude
      end

      # overwritten
      def migration_storage_name
        :schema_info
      end

    end
  end
end


