require "rubygems"
require "bundler/setup" 
require 'sequel'
require 'csv'
require 'logger'
$:<<'lib'

#DB = Sequel.connect "postgres://your_connection_string"
DB = Sequel.connect "postgres:/census"
require 'census.rb'

DB.loggers << ::Logger.new('/tmp/db2.log')
extend Census
DB.run("set client_encoding to 'LATIN-1'")

census_dir='census/'

create_geoheader_table(Dir["#{census_dir}/**/*All_Geographies*/g2010*.txt"])
create_lookup_tables('census/Sequence_Number_and_Table_Number_Lookup.txt')

create_all_estimate_tables

#Census::CensusColumnLookup.table_names.each do |t|
#  DB[t.to_sym].truncate
#end

sequences=Dir["#{census_dir}/**/e20105ca**.txt"].sort.map do |s|
  Census::CensusSequenceFile.new(s)
end

sequences.each do |s|
  puts "loading #{s.filename}"
  s.load_tables :all, :use_copy => false
end

