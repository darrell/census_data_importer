require 'csv'
module Census
  class CensusSequenceFile
    attr_reader :type,:year,:state,:period,:sequence,:iteration,:filename
    def initialize(file)
      if file =~ /([em])(\d{4})(\d)(\w+)(\d{4})(\d{3}).txt$/
        @type=$1=='m'? 'margin' : 'estimate'
        @year=$2
        @period=$3
        @state=$4
        @sequence=$5
        @iteration=$6
        @filename=file
      else
        raise "Could not interpret filename. Are you sure this is census data?"
      end
    end

    def cleanup_row(row)
      row.map do |v|
        case v
        when '' then nil
        when '.' then -2
        else v
        end
      end
    end
    # return a hash containing a table_name and a Range for the columns that go with that table:
    # e.g. {:table1 => 7..15, :table2 => 16..32}
    def table_ranges(table=:all)
      ret={}
      ds=CensusColumnLookup.select(Sequel.function(:max, :line_no).as(:stop), 
                                  :start_pos, :table_id).group(:start_pos,:table_id).filter(:sequence_no => @sequence)

      ds=ds.filter({:table_id => table}) if  table != :all

      ds.each do |r|
        start=r[:start_pos]-1
        stop=r[:start_pos]-1+r[:stop]-1
        ret[r[:table_id]]=start..stop
      end
      ret
    end
    
    # return a hash with all rows and columns for a given table (i.e. array of arrays)
    # only tables defined in this sequence files be returned.
    def get_tables(tables=:all,opts={})
      opts[:as_csv]=opts.fetch(:as_csv,false)
      all_tables={}
      tr=self.table_ranges(tables)
   
      # shortcut
      return tr if tr == {}
   
      #initialize
      tr.keys.each do |k|
        all_tables[k]=[]
      end
      # read the file
      CSV.foreach(@filename) do |row|
        row = cleanup_row(row)
        tr.each do |k,range|
          all_tables[k] << row[0..5]+row[range]
        end
      end
      # remove tables that may have been asked for, 
      # but don't exist in this file, also
      # remove any potentially duplicate rows
      all_tables.each do |k,v|
        v.uniq!
        all_tables.delete(k) if v.empty?
      end
      if opts[:as_csv]
        all_tables.each do |table,rows|
          rows.map!{|r| r.to_csv}
        end
      else
        all_tables
      end
    end

   # load into appropriate DB tables
   # each census table in this sequence file
   # options: :use_copy use COPY instead of INSERT (default true in postgres)
   # :ignore_dups (ignore errors about duplicate keys, :default true)
   #    primarily makes sense if :use_copy is false
   #    if :use_copy is true, all errors will be ignored, which may not
   #    be what you want.
   def load_tables(tables = :all, opts={})
     opts[:use_copy]=opts.fetch(:use_copy,true)
     opts[:ignore_dups]=opts.fetch(:ignore_dups,false)
     t=get_tables(tables, :as_csv => opts[:use_copy])
     if @type == 'margin'
       x={}
       t.each_pair{|k,v| x["#{k}_moe"]=v;  t.delete(k)}
       t=x
     end
     t.each do |table,rows|
       DB.log_info "Loading table '#{table}' from sequence '#{@filename}'"
       if opts[:use_copy]
         begin
           DB.copy_into table.to_sym, :data => rows, :format => :csv, :options => "ENCODING 'LATIN-1'"
         rescue => e
           DB.log_info "Failed to load table '#{table}' from sequence '#{@filename}': #{e.message}" 
           raise e unless opts[:ignore_dups]
         end
       else
         DB.transaction do
           rows.each do |row|
             if ! opts[:ignore_dups]
               DB[table.to_sym].insert(row)
             else
               DB.transaction(:savepoint => true) do 
                 begin
                   DB[table.to_sym].insert(row)
                 rescue Sequel::DatabaseError => err
                   raise err unless err.message =~ /duplicate key/
                   raise Sequel::Rollback
                 end 
               end #savepoint
             end #if :ignore_dups
           end
         end
       end
     end
   end
      
   def inspect
      %Q{
        type: #{type}
        year: #{year}
        state: #{state}
        period: #{period}
        sequence: #{sequence}
        iteration: #{iteration}
      }
   end
   
    def open(*args, &block)
      File.open(@filename, *args, &block)
    end
       
  end

  class CensusColumn
    # expects a hash, as from Sequel::Dataset
    attr_reader :name,:id,:table_id

    def pad_sequence(x)
      # apparently, it's hard to pad out a float just to the left of the period
      f=sprintf('%.1f',x).split('.')
      f[0]=f[0].rjust(4,'0')
      f[1]=nil if f[1] =~ /^0+$/
      f.compact.join('.')
    end

    def initialize(row)
      @id = row[:table_id]+pad_sequence(row[:line_no])
      @name=row[:table_title]
      @table_id=row[:table_id]
    end
  end

  class CensusTable
    attr_accessor :columns
    attr_reader :id
    def initialize(id,cols=[])
      @id=id
      @columns = []+cols
    end
  end

  class CensusLookup < Sequel::Model(:census_lookup)
    def self.tables(tables=:all)
      t=[]
      ds=self.filter(~{:start_pos => nil} & ~{:cells_in_table => nil})
      if tables != :all
        ds=ds.filter({:table_id => tables})
      end
      ds.each do |table|
        x=CensusTable.new(table[:table_id])
        # sometimes a table is in more than one sequence file.
        ds=self.filter({Sequel.function(:mod, :line_no,1) => 0} & {:table_id => table[:table_id]}).distinct(:table_id,:line_no).each do |c|
          x.columns << CensusColumn.new(c)
        end
        t<<x
      end
      return t
    end
  end

  class CensusColumnLookup < Sequel::Model(:census_column_lookup)

    # return a list of all tables in given sequence number(s) or :all (default)
    def self.table_names(seq=:all)
      ret=[]
      ds=self.select(:table_id).distinct
      if seq != :all
        ds=ds.filter(:sequence_no => seq)
      end
      ds.each do |x|
        ret<<x[:table_id]
      end
      ret
    end

    def self.columns(table)
      self.filter({:table_id => table})
    end
  end


  def parse_geography_file(f)
    ret=[]
    # the size of each field
    fields = [6,2,3,2,7,1,1,1,2,2,3,5,5,6,1,5,4,5,1,3,5,5,5,3,5,1,1,5,3,5,5,5,2,3,3,6,3,5,5,5,5,5,1,1,6,5,5,5,40,200,6,1,43] 
    field_pattern = "A#{fields.join('A')}"
    File.open(f).each do |row|
      # puts row
      ret << row.unpack(field_pattern).map{|x|x.strip ; x.empty? ? nil : x}
    end
    ret #.map{|x| x.join("\t")}
  end
  
  def create_lookup_tables(file, use_copy = false)
    DB.create_table! :census_lookup do
      primary_key :id
      String :file_id, :size => 10
      String :table_id, :size => 10
      Integer :sequence_no, :size => 10
      Numeric :line_no
      Integer :start_pos
      String :cells_in_table, :size => 30
      Integer :cells_in_sequence
      String :table_title
      String :subject_area
    end
    cols=[:file_id, :table_id, :sequence_no,
            :line_no, :start_pos, :cells_in_table, :cells_in_sequence, 
            :table_title, :subject_area]
    if use_copy
      DB.copy_into(:census_lookup, 
        :columns => cols,
        :format => :csv, 
        :options => "HEADER,ENCODING 'LATIN-1'",
        :data => File.open(file))
    else
      CSV.open(file, 'r:ISO-8859-1',:headers => true).each do |row|
        x = row.fields.map{|v| v=='.' || v=='' ? nil : v}
        # puts Hash[cols.zip(x)]
        DB[:census_lookup].insert Hash[cols.zip(x)]

      end
    end



    DB.drop_table? :census_column_lookup
    DB.run %q{SELECT c.id,file_id,c.table_id,sequence_no,c.table_id||lpad(line_no::text,4,'0') as column_id,
            c.line_no::integer,
            s.start_pos,regexp_replace(s.cells_in_table,E' *CELLS*.*','','g')::integer as cells_in_table,
            c.table_title as column_title,topic,table_universe,c.subject_area
      INTO TABLE census_column_lookup
      FROM census_lookup c
      JOIN (
        SELECT table_id,cells_in_table,start_pos,table_title as topic,subject_area
        FROM census_lookup
        WHERE cells_in_table IS NOT NULL
          AND start_pos IS NOT NULL
        ) as s ON (s.table_id=c.table_id)
        JOIN (
        SELECT table_id,regexp_replace(table_title, E'^Universe:[ ]+','') as table_universe
          FROM census_lookup
          WHERE cells_in_table IS NULL
            AND start_pos IS NULL
            AND line_no IS NULL
          ) as t ON (t.table_id=c.table_id)

      WHERE line_no IS NOT NULL
      AND mod(line_no::numeric,1)=0
      ORDER BY sequence_no,c.table_id,line_no
      --LIMIT 100}
    
  end

  def create_all_estimate_tables(suffix='')
    float_tables=%w{
      B01002 B01002A B01002B B01002C B01002D B01002E B01002F B01002G
      B01002H B01002I B05004 B06002 B07002 B07402 B08103 B08503 B12007
      B19082 B19083 B23013 B23020 B25010 B25018 B25021 B25071 B25092
      B98011 B98012 B98021 B98022 B98031 B98032
    }

    Census::CensusLookup.tables.each do |t|
      DB.create_table!("#{t.id}#{suffix}".to_sym) do
        String :fileid, :size=>6
        String :filetype, :size=>6
        String :stusab, :size=>2, :null=>false
        String :chariter, :size=>3
        String :seq, :size=>4
        Integer :logrecno, :null=>false
        t.columns.each do |c|
          if float_tables.include? t.id.to_s
            Float c.id
          else
            Integer c.id
          end
        end
        primary_key [:stusab, :logrecno]
      end
    end
  end
  
  def create_all_error_tables
    create_all_estimate_tables('_moe')
  end
  

    def cleanup_row(row)
      row.map do |v|
        case v
        when '' then nil
        when '.' then -2
        else v
        end
      end
    end
  def create_geoheader_table(files, use_copy = true)
    DB.create_table!(:geoheader) do
      String :fileid, :size=>6
      String :stusab, :size=>2, :null=>false
      Integer :sumlevel
      String :component, :size=>2
      Integer :logrecno, :null=>false
      String :us, :size=>1
      String :region, :size=>1
      String :division, :size=>1
      String :statece, :size=>2
      String :state, :size=>2
      String :county, :size=>3
      String :cousub, :size=>5
      String :place, :size=>5
      String :tract, :size=>6
      String :blkgrp, :size=>1
      String :concit, :size=>5
      String :aianhh, :size=>4
      String :aianhhfp, :size=>5
      String :aihhtli, :size=>1
      String :aitsce, :size=>3
      String :aits, :size=>5
      String :anrc, :size=>5
      String :cbsa, :size=>5
      String :csa, :size=>3
      String :metdiv, :size=>5
      String :macc, :size=>1
      String :memi, :size=>1
      String :necta, :size=>5
      String :cnecta, :size=>3
      String :nectadiv, :size=>5
      String :ua, :size=>5
      String :blank1, :size=>5
      String :cdcurr, :size=>2
      String :sldu, :size=>3
      String :sldl, :size=>3
      String :blank2, :size=>6
      String :blank3, :size=>3
      String :blank4, :size=>5
      String :submcd, :size=>5
      String :sdelm, :size=>5
      String :sdsec, :size=>5
      String :sduni, :size=>5
      String :ur, :size=>1
      String :pci, :size=>1
      String :blank5, :size=>6
      String :blank6, :size=>5
      String :puma5, :size=>5
      String :blank7, :size=>5
      String :geoid, :size=>40
      String :name, :size=>200
      String :bttr, :size=>6
      String :btbg, :size=>1
      String :blank8, :size=>50
      primary_key [:stusab, :logrecno]
      index :geoid, :unique=>true
  end

    files.sort.each do |s|
      DB.log_info "Processing geoheader #{s}"
      DB.transaction do 
        csv=[]
        parse_geography_file(s).each do |row|
          row = cleanup_row(row)
          if use_copy
            csv << row.to_csv
          else
            DB[:geoheader].insert row
          end
        end
        if use_copy
          DB.copy_into :geoheader, :data => csv, :format => :csv, :options => "ENCODING 'LATIN-1'"
        end
      end
    end

    DB.alter_table(:geoheader) do
      add_column  :geoid_tiger, String, :size=>40
      add_index   :geoid_tiger
    end

    # for some unbeknowst reason, the geoheader is upcase whereas the
    # data tables are downcased.
    # for some different unbeknownst reason, the geoid field is not
    # directly joinable to the geoid in the TIGER data, so we create
    # a field for that, which we can join on.
    DB[:geoheader].update(:stusab => Sequel.function(:lower, :stusab),
                          :geoid_tiger => Sequel.function(:split_part,:geoid,'US',2))

  end
end
