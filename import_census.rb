require "rubygems"
require "bundler/setup" 
require 'sequel'
require 'csv'
require 'logger'
$:<<'lib'

DB = Sequel.connect "postgres://your_connection_string"

# keep our importing happy, since the Census doesn't
# see fit to just use UTF-8.
DB.run("set client_encoding to 'LATIN-1'")

# log commands to a file so we can see what's happening
DB.loggers << ::Logger.new('acs2011_5yr.log')

# load the commands up
require 'census.rb'
extend Census

# we want to stick everything in the acs2011_5yr schema
DB.run("set search_path to acs2011_5yr")

# where our data is kept
census_dir='census/'

# create the geoheader table
# NOTE: this table is dropped each time, so
#  be sure you include all geoheader files you might be using
create_geoheader_table(Dir["#{census_dir}/**/*All_Geographies*/g20*.txt"])

# load the census_lookup table and from that, generate the census_column_lookup table
create_lookup_tables('census/ACS2011_5yr_Sequence_Number_and_Table_Number_Lookup.txt')

# use the census_column_lookup table to create all the estimate tables
create_all_estimate_tables

# truncate all the tables before we load data into them
# Census::CensusColumnLookup.table_names.each do |t|
#  DB[t.to_sym].truncate
# end

# import ACS 2011 5yr data.
sequences=Dir["#{census_dir}/**/*20115*.txt"].sort.map do |s|
  Census::CensusSequenceFile.new(s)
end

sequences.each do |s|
  puts "loading #{s.filename}"
  s.load_tables :all, :use_copy => true
end

# If you get errors, this is a safe, if not exactly speedy
# way to fill in missing data
#
# sequences.each do |s|
#   puts "loading #{s.filename}"
#   s.load_tables :all, :use_copy => false, :ignore_dups => true
# end

