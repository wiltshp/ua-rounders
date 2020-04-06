#add .all to any query to see results displayed.
#I.E. @raw_data_table.order(:game_date).select_group(:game_date, :big_cash).exclude(big_cash: nil).all
def extract_files_from_zip
  FileUtils.rm Dir.glob("#{$data_directory}/raw_data_files/*.xlsx")

  Dir.glob("#{$data_directory}/zip_files/*.zip").each do |zip_file_name|
    Zip::File.open(zip_file_name) do |zip_file_contents|
      zip_file_contents.each do |entry|
        entry.extract("#{$data_directory}/raw_data_files/#{entry.name}")
      end
    end
  end

end

def create_database_tables
  @database_handle = SQLite3::Database.open(':memory:')#, :loggers => [Logger.new($stdout)]

  @database_handle.execute("DROP TABLE IF EXISTS `raw_data_table`")
  @database_handle.execute("DROP VIEW IF EXISTS `curr_year_raw_data_table`")
  @database_handle.execute("DROP VIEW IF EXISTS `prev_year_raw_data_table`")

  @database_handle.execute("CREATE TABLE `raw_data_table` (`game_date` date NOT NULL, `venue` varchar(255), `game_on` varchar(255) NOT NULL, `player` varchar(255) NOT NULL, `attend` varchar(255), `eat` varchar(255), `big_cash` varchar(255))")
  @database_handle.execute("CREATE UNIQUE INDEX `raw_data_table_game_date_player_index` ON `raw_data_table` (`game_date`, `player`)")
  @database_handle.execute("CREATE INDEX `raw_data_table_game_date_index` ON `raw_data_table` (`game_date`)")
  
  query="CREATE VIEW `curr_year_raw_data_table` AS SELECT * FROM `raw_data_table` WHERE (`game_date` >= '#{$current_year}')"
  #query=query.to_s
  @database_handle.execute(query)


  #@database_handle.create_view(:curr_year_raw_data_table, @raw_data_table.where { game_date >= $current_year })
  #@curr_year_raw_data_table = @database_handle[:curr_year_raw_data_table]

  begin_date = $current_year.prev_year
  end_date = $current_year
  @database_handle.create_view(:prev_year_raw_data_table, @raw_data_table.where { game_date >= begin_date }.where { game_date < end_date })
  @prev_year_raw_data_table = @database_handle[:prev_year_raw_data_table]
end

def load_and_validate_data
  valid_hosts = ['Tom','Uncle Mike', 'Eric Kohl', 'PT', 'Hoy', 'Creamy', 'Harrisons', nil]
  Dir.glob("#{$data_directory}/raw_data_files/*.xlsx").sort.each do |file_name|
    source_spreadsheet = Roo::Spreadsheet.open(file_name).sheet(0)
    host_location = source_spreadsheet.row(1)[1]
    abort("#{file_name} contains an invalid host location of #{host_location}") unless valid_hosts.include?(host_location)
    date_of_game = source_spreadsheet.row(1)[3]
    abort("#{file_name} contains a host date of #{date_of_game} which is not a valid thursday") unless date_of_game.thursday?
    game_on = source_spreadsheet.row(2)[1]
    abort("#{file_name} contains an invalid game on indicator of #{game_on}") unless ['Yes', 'No'].include?(game_on)
    if game_on == 'Yes'
      abort("#{file_name} contains a game on indicator of yes, but no big cash is defined") if source_spreadsheet.row(2)[6].nil?
      big_cash = source_spreadsheet.row(2)[6].strip
    else
      big_cash = nil
    end

    ((source_spreadsheet.first_row + 3)..source_spreadsheet.last_row).each do |row|

      name = source_spreadsheet.row(row)[0].strip
      attended = source_spreadsheet.row(row)[1]
      abort("#{file_name} contains an invalid attend indicator of #{attended} for #{name}") unless ['Yes', 'No', nil].include?(attended)
      eat = source_spreadsheet.row(row)[2]
      abort("#{file_name} contains an invalid eat indicator of #{eat} for #{name}") unless ['Yes', 'No', nil].include?(eat)

      @raw_data_table.insert(:game_date => date_of_game, :venue => host_location, :game_on => game_on, :player => name, :attend => attended, :eat => eat, :big_cash => big_cash)

    end

  end

  first_thur = Date.parse @raw_data_table.min(:game_date)
  last_thur = Date.parse @raw_data_table.max(:game_date)
  @valid_thursdays = (first_thur..last_thur).select { |day| day.thursday? }

  @valid_thursdays.each do |day|
    abort("Missing Spreadsheet for #{day}") unless @raw_data_table.where(:game_date => day).count > 0
  end

  names_to_be_changed = {"Greg" => '#Copper', "Old Hummus" => 'Digital Hummus', "Digitial Hummus" => 'Digital Hummus', "Baldo" =>  'Nick Engel'}
  names_to_be_changed.each { |key, value|
    @raw_data_table.where(:venue => key).update(:venue => value)
    @raw_data_table.where(:player => key).update(:player => value)
    @raw_data_table.where(:big_cash => key).update(:big_cash => value)
  }

end

def create_raw_data_sheets
  sheet = $target_workbook.create_worksheet(:name => 'All Raw Data')
  date_format = Spreadsheet::Format.new :number_format => 'MM/DD/YYYY'
  sheet.column(0).default_format = date_format
  sheet.row(0).push 'Date', 'Venue', 'Game On', 'Player', 'Attendance', 'Eat', 'Big Cash'
  count = 1
  @raw_data_table.each do |row|
    sheet.row(count).concat row.values
    count = count + 1
  end

  sheet = $target_workbook.create_worksheet(:name => 'Prev Year Raw Data')
  date_format = Spreadsheet::Format.new :number_format => 'MM/DD/YYYY'
  sheet.column(0).default_format = date_format
  sheet.row(0).push 'Date', 'Venue', 'Game On', 'Player', 'Attendance', 'Eat', 'Big Cash'
  count = 1
  @prev_year_raw_data_table.each do |row|
    sheet.row(count).concat row.values
    count = count + 1
  end

  sheet = $target_workbook.create_worksheet(:name => 'Curr Year Raw Data')
  date_format = Spreadsheet::Format.new :number_format => 'MM/DD/YYYY'
  sheet.column(0).default_format = date_format
  sheet.row(0).push 'Date', 'Venue', 'Game On', 'Player', 'Attendance', 'Eat', 'Big Cash'
  count = 1
  @curr_year_raw_data_table.each do |row|
    sheet.row(count).concat row.values
    count = count + 1
  end
end

def create_attendance_count_sheet
  #Retrieve player count by game date where the game on indicator is 'Yes' and the player attendance is 'Yes'
  sheet = $target_workbook.create_worksheet(:name => 'Attendance')
  sheet.row(0).push 'Timeframe', 'Player', 'Attendance Count'
  count = 1

  $list_of_tables.each do |table_name|
    sql = "select case '#{table_name}'#{$data_type_string}
          player, count(1) as attendance_count
          from #{table_name}
          where attend = 'Yes'
          and game_on = 'Yes'
          group by player
          order by 3 desc, 2 asc"
    @database_handle.fetch(sql) do |row|
      sheet.row(count).push row[:status], row[:player], row[:attendance_count]
      count = count+1
    end
    count = count+1
  end
end

def create_host_count_sheet
  #Retrieve host location by game date where the game on indicator is 'Yes'
  sheet = $target_workbook.create_worksheet(:name => 'Host')
  sheet.row(0).push 'Timeframe', 'Venue', 'Host Count'
  count = 1

  $list_of_tables.each do |table_name|
    sql = "select case '#{table_name}'#{$data_type_string}
           venue, count(1) as host_count
          from (select venue, game_date
                from #{table_name}
                where game_on = 'Yes'
                group by venue, game_date)
          group by venue
          order by 3 desc, 2 asc"
    @database_handle.fetch(sql) do |row|
      sheet.row(count).push row[:status], row[:venue], row[:host_count]
      count = count+1
    end
    count = count+1
  end
end

def create_big_cash_count_sheet
  #Retrieve big cash by game date where the game on indicator is 'Yes'
  sheet = $target_workbook.create_worksheet(:name => 'Big Cash')
  sheet.row(0).push 'Timeframe', 'Player', 'Total Big Cash Count'
  count = 1

  $list_of_tables.each do |table_name|
    sql = "select case '#{table_name}'#{$data_type_string}
  big_cash, count(1) as cash_count
          from (select big_cash, game_date
                from #{table_name}
                where game_on = 'Yes'
                group by big_cash, game_date)
          group by big_cash
          order by 3 desc, 2 asc"
    @database_handle.fetch(sql) do |row|
      sheet.row(count).push row[:status], row[:big_cash], row[:cash_count]
      count = count+1
    end
    count = count+1
  end


  #Retrieve big cash by game date where the game on indicator is 'Yes' and host location is at Hoy's, Doc's or Uncle Mike's
  count = count+1
  sheet.row(count).push "Big Cash outside the bubble"
  count = count+1

  $list_of_tables.each do |table_name|
    sql = "select case '#{table_name}'#{$data_type_string}
 big_cash, count(1) as cash_count
          from (select big_cash, game_date
                from #{table_name}
                where game_on = 'Yes'
                and venue in (#{$outside_of_ua})
                group by big_cash, game_date)
          group by big_cash
          order by 3 desc, 2 asc"
    @database_handle.fetch(sql) do |row|
      sheet.row(count).push row[:status], row[:big_cash], row[:cash_count]
      count = count+1
    end
    count = count+1
  end


  #Retrieve big cash by game date where the game on indicator is 'Yes' and host location is not at Hoy's, Doc's or Uncle Mike's
  count = count+1
  sheet.row(count).push "Big Cash inside the bubble"
  count = count+1

  $list_of_tables.each do |table_name|
    sql = "select case '#{table_name}'#{$data_type_string}
 big_cash, count(1) as cash_count
          from (select big_cash, game_date
                from #{table_name}
                where game_on = 'Yes'
                and venue not in (#{$outside_of_ua})
                group by big_cash, game_date)
          group by big_cash
          order by 3 desc, 2 asc"
    @database_handle.fetch(sql) do |row|
      sheet.row(count).push row[:status], row[:big_cash], row[:cash_count]
      count = count+1
    end
    count = count+1
  end

end

def create_big_cash_by_attend_percentage_sheet
  sheet = $target_workbook.create_worksheet(:name => 'Big Cash by Attendance')
  sheet.row(0).push 'Timeframe', 'Player', 'Attended', 'Big Cash Count', 'Percent Big Cash'
  count = 1

  $list_of_tables.each do |table_name|
    sql = "select case '#{table_name}'#{$data_type_string}
 games_played.player as player, games_played.attendance_count as attend_count, ifnull(big_host_data.cash_count,0) as big_cash_count, ifnull(round(cast(big_host_data.cash_count as float)/cast(games_played.attendance_count as float)*100,1),0) as percentage_big_cash
          from (select player, count(1) as attendance_count
                from #{table_name}
                where attend = 'Yes'
                and game_on = 'Yes'
                group by player) games_played left outer join
               (select big_cash, count(1) as cash_count
                from (select big_cash, game_date
                      from #{table_name}
                      where game_on = 'Yes'
                      group by big_cash, game_date)
                group by big_cash ) big_host_data
               on big_host_data.big_cash = games_played.player
          order by percentage_big_cash desc, attend_count desc, player asc"
    @database_handle.fetch(sql) do |row|
      sheet.row(count).push row[:status], row[:player], row[:attend_count], row[:big_cash_count], row[:percentage_big_cash]
      count = count+1
    end
    count = count+1
  end

end

def create_total_hours_played_sheet
  #Retrieve player attendance count times 4(hours) where the game on indicator is 'Yes' and the player attendance is 'Yes'
  sheet = $target_workbook.create_worksheet(:name => 'Hours Played')
  sheet.row(0).push 'Timeframe', 'Player', 'Time Played'
  count = 1

  $list_of_tables.each do |table_name|
    sql = "select case '#{table_name}'#{$data_type_string}
 player, count(1) * 4 as hours_played
          from #{table_name}
          where attend = 'Yes'
          and game_on = 'Yes'
          group by player
          order by 3 desc, 2 asc"
    @database_handle.fetch(sql) do |row|
      dd, hh = row[:hours_played].divmod(24)
      string = ''
      if dd > 0
        string += '%d days' % [dd]
      end
      if dd > 0 and hh > 0
        string += ' and %d hours' % [hh]
      end
      if dd == 0 and hh > 0
        string += '%d hours' % [hh]
      end

      sheet.row(count).push row[:status], row[:player], string
      count = count+1
    end
    count = count+1
  end

end

def create_responds_least_amount_sheet
  #Retrieve player attendance count where the game on indicator is 'Yes' and the player attendance is null
  sheet = $target_workbook.create_worksheet(:name => 'Responds Least')
  sheet.row(0).push 'Timeframe', 'Player', 'Not Responded Count'
  count = 1

  $list_of_tables.each do |table_name|
    sql = "select case '#{table_name}'#{$data_type_string}
 player, count(1) as not_respond_count
          from (select player, game_date
                from #{table_name}
                where game_on = 'Yes'
                and attend is null
                group by player, game_date)
          group by player order by 3 desc, 2 asc"
    @database_handle.fetch(sql) do |row|
      sheet.row(count).push row[:status], row[:player], row[:not_respond_count]
      count = count+1
    end
    count = count+1
  end

end

def create_average_player_count_by_venue_sheet
  #Retrive average count of players by host location where game on indicator = 'Yes'
  sheet = $target_workbook.create_worksheet(:name => 'Average Player')
  sheet.row(0).push 'Timeframe', 'Host', 'Average Player Count'
  count = 1

  $list_of_tables.each do |table_name|
    sql = "select case '#{table_name}'#{$data_type_string}
 venue, round(avg(num_players),1) as avg_player_count
          from (select venue, game_date
                from #{table_name}
                where game_on = 'Yes'
                group by venue, game_date) a,
               (select game_date, count(*) as num_players
                from #{table_name}
                where game_on = 'Yes'
                and attend = 'Yes'
                group by game_date) b
          where a.game_date = b.game_date
          group by venue
          order by avg_player_count desc"
    @database_handle.fetch(sql) do |row|
      sheet.row(count).push row[:status], row[:venue], row[:avg_player_count]
      count = count+1
    end
    count = count+1
  end

end

def create_games_played_percentage_sheet
  sheet = $target_workbook.create_worksheet(:name => 'Games Played')
  sheet.row(0).push 'Timeframe', 'Total Games Possible', 'Game On', 'Game Off', 'Percent of Game On', 'Percent of Game Off'
  count = 1

  $list_of_tables.each do |table_name|
    sql = "select case '#{table_name}'#{$data_type_string}
 c.total_games, a.games_had, b.games_not_had, round(cast(a.games_had as float)/cast(c.total_games as float)*100,1) as game_on,
         round(cast(b.games_not_had as float)/cast(c.total_games as float)*100,1) as game_off
         from (select count(distinct game_date) as games_had
               from #{table_name}
               where game_on = 'Yes') a,
              (select count(distinct game_date) as games_not_had
               from #{table_name}
               where game_on = 'No') b,
              (select count(distinct game_date) as total_games
               from #{table_name} ) c"
    @database_handle.fetch(sql) do |row|
      sheet.row(count).push row[:status], row[:total_games], row[:games_had], row[:games_not_had], row[:game_on], row[:game_off]
      count = count+1
    end
    count = count+1
  end

end

def create_number_of_games_in_ua_sheet
  sheet = $target_workbook.create_worksheet(:name => 'UA Hosted Games')
  sheet.row(0).push 'Timeframe', 'Total Games Played', 'Games in UA', 'Games not in UA', 'Percent of Game UA', 'Percent of Game Not UA'
  count = 1

  $list_of_tables.each do |table_name|
    sql = "select case '#{table_name}'#{$data_type_string}
 c.total_games, a.games_in_ua, b.games_not_in_ua, round(cast(a.games_in_ua as float)/cast(c.total_games as float)*100,1) as game_ua,
         round(cast(b.games_not_in_ua as float)/cast(c.total_games as float)*100,1) as game_not_ua
         from (select count(distinct game_date) as games_in_ua
               from #{table_name}
               where game_on = 'Yes'
               and venue not in (#{$outside_of_ua})) a,
              (select count(distinct game_date) as games_not_in_ua
               from #{table_name}
               where game_on = 'Yes'
               and venue in (#{$outside_of_ua})) b,
              (select count(distinct game_date) as total_games
               from #{table_name}
               where game_on = 'Yes') c"
    @database_handle.fetch(sql) do |row|
      sheet.row(count).push row[:status], row[:total_games], row[:games_in_ua], row[:games_not_in_ua], row[:game_ua], row[:game_not_ua]
      count = count+1
    end
    count = count+1
  end

end

def create_horsepower_sheet
  sheet = $target_workbook.create_worksheet(:name => 'Horse Power')
  sheet.row(0).push 'Player', 'Car', 'HorsePower'
  sheet.row(1).push 'Carle', 'Ford Shelby GT500', '540'
  sheet.row(2).push 'PT', 'BMW M5', '500'
  sheet.row(3).push 'Creamy', 'Toyota Tundra', '381'
  sheet.row(4).push 'Doc', 'Ford F-150', '360'
  sheet.row(5).push 'Capt.', 'Volvo S80', '300'
  sheet.row(6).push 'Greg', 'Toyota 4 Runner', '270'
  sheet.row(7).push 'Tom', 'Lexus GX 470', '263'
  sheet.row(8).push 'Hoy', 'Honda Ridgeline', '245'
  sheet.row(9).push 'Fink', 'Acura TL', '225'
  sheet.row(10).push 'Uncle Mike', 'Jeep Liberty', '210'
end

def create_distance_from_scioto_sheet
  sheet = $target_workbook.create_worksheet(:name => 'Scioto Distance')
  sheet.row(0).push 'Player', 'Distance from Scioto'
  sheet.row(1).push 'PT', '1.4 Miles'
  sheet.row(2).push 'Tom', '1.5 Miles'
  sheet.row(3).push 'Greg', '1.5 Miles'
  sheet.row(4).push 'Creamy', '1.7 Miles'
  sheet.row(5).push 'Fink', '1.8 Miles'
  sheet.row(6).push 'Carle', '2.1 Miles'
  sheet.row(7).push 'Hoy', '5.8 Miles'
  sheet.row(8).push 'Uncle Mike', '15.7 Miles'
  sheet.row(9).push 'Capt.', '28.9 Miles'
  sheet.row(10).push 'Doc', '36.4 Miles'
end

def create_number_of_divorces_sheet
  sheet = $target_workbook.create_worksheet(:name => 'Divorce Count')
  sheet.row(0).push 'Player', 'Number of Divorces'
  sheet.row(1).push 'Uncle Mike', '3 Divorces'
  sheet.row(2).push 'Cakes', '1 Divorce'
  sheet.row(3).push 'Carle', '1 Divorce'
  sheet.row(4).push 'Doc', '1 Divorce'
  sheet.row(5).push 'Capt.', '0 Divorces'
  sheet.row(6).push 'Creamy', '0 Divorces'
  sheet.row(7).push 'Fink', '0 Divorces'
  sheet.row(8).push 'Greg', '0 Divorces'
  sheet.row(9).push 'Hoy', '0 Divorces'
  sheet.row(10).push 'PT', '0 Divorces'
  sheet.row(11).push 'Tom', '0 Divorces'

  sheet.row(13).push 'People Count', 'Number of Divorces', 'Location'
  sheet.row(14).push '6', '5', 'NOZ UA Bubble'
  sheet.row(15).push '5', '0', 'SOZ UA Bubble'
end

def create_hoy_fart_sheet
  sheet = $target_workbook.create_worksheet(:name => 'Hoy Fart')
  counter = 3
  sheet.row(0).push "If on average Hoy farts #{counter} times during a game he's present, then..."
  count = 1

  $list_of_tables.each do |table_name|
    sql = "select case '#{table_name}'#{$data_type_string}
 player,'has consumed' as string1, count(1)*#{counter} as sql_counter, 'Hoy farts' as string2
          from #{table_name}
          where attend = 'Yes'
          and game_on = 'Yes'
          and game_date in (select game_date from #{table_name} where game_on = 'Yes' and player = 'Hoy' and attend = 'Yes')
          group by player
          order by 4 desc, 2 asc"
    @database_handle.fetch(sql) do |row|
      sheet.row(count).push row[:status], row[:player], row[:string1], row[:sql_counter], row[:string2]
      count = count+1
    end
    count = count+1
  end
end

def create_tom_peed_sheet
  sheet = $target_workbook.create_worksheet(:name => 'Tom Peed')
  counter = 2
  sheet.row(0).push "If on average you pee twice while at Tom's house, then..."
  count = 1

  $list_of_tables.each do |table_name|
    sql = "select case '#{table_name}'#{$data_type_string}
 player,'has pee`d' as string1, count(1)*#{counter} as sql_counter, 'times in Tom`s yard' as string2
          from #{table_name}
          where attend = 'Yes'
          and game_on = 'Yes'
          and venue = 'Tom'
          group by player
          order by 4 desc, 2 asc"
    @database_handle.fetch(sql) do |row|
      sheet.row(count).push row[:status], row[:player], row[:string1], row[:sql_counter], row[:string2]
      count = count+1
    end
    count = count+1
  end

end

def create_mike_bluh_sheet
  sheet = $target_workbook.create_worksheet(:name => 'Mike Uhhh')
  counter = 5
  sheet.row(0).push "If on average Uncle Mike says Bluhhhhh #{counter} times during a game he's present, then..."
  count = 1

  $list_of_tables.each do |table_name|
    sql = "select case '#{table_name}'#{$data_type_string}
 player,'has heard Uncle Mike say Bluhhhhh' as string1, count(1)*#{counter} as sql_counter, 'times' as string2
          from #{table_name}
          where attend = 'Yes'
          and game_on = 'Yes'
          and game_date in (select game_date from #{table_name} where game_on = 'Yes' and player = 'Uncle Mike' and attend = 'Yes')
          group by player
          order by 4 desc, 2 asc"
    @database_handle.fetch(sql) do |row|
      sheet.row(count).push row[:status], row[:player], row[:string1], row[:sql_counter], row[:string2]
      count = count+1
    end
    count = count+1
  end

end

def create_date_last_played_sheet
  sheet = $target_workbook.create_worksheet(:name => 'Date Last Played')
  date_format = Spreadsheet::Format.new :number_format => 'MM/DD/YYYY'
  sheet.column(1).default_format = date_format
  sheet.row(0).push 'Timeframe', 'Player', 'Last Played Date', 'Days Since Last Played', 'Weeks Since Last Played'
  count = 1

  $list_of_tables.each do |table_name|
    sql = "select case '#{table_name}'#{$data_type_string}
 player, max(game_date) as last_played_date,
julianday((select max(game_date) from #{table_name} where game_on = 'Yes'))-julianday(max(game_date))||' days' as days_since_last_played,
julianday((select max(game_date) from #{table_name} where game_on = 'Yes'))/7-julianday(max(game_date))/7||' weeks' as weeks_since_last_played
          from #{table_name}
          where attend = 'Yes'
          and game_on = 'Yes'
          group by player
          order by 3 asc, 2 asc"
    @database_handle.fetch(sql) do |row|
      sheet.row(count).push row[:status], row[:player], row[:last_played_date], row[:days_since_last_played], row[:weeks_since_last_played]
      count = count+1
    end
    count = count+1
  end

end

def create_eat_most_sheet
  #Retrieve eat count by game date where the game on indicator is 'Yes' and the player attendance is 'Yes' and the eat indicator = 'Yes'
  sheet = $target_workbook.create_worksheet(:name => 'Eat the Most')
  sheet.row(0).push 'Timeframe', 'Player', 'Eating Count'
  count = 1

  $list_of_tables.each do |table_name|
    sql = "select case '#{table_name}'#{$data_type_string}
 player, count(1) as eat_count
          from #{table_name}
          where attend = 'Yes'
          and game_on = 'Yes'
          and eat = 'Yes'
          group by player
          order by 3 desc, 2 asc"
    @database_handle.fetch(sql) do |row|
      sheet.row(count).push row[:status], row[:player], row[:eat_count]
      count = count+1
    end
    count = count+1
  end
end

def create_back_to_back_winners
  sheet = $target_workbook.create_worksheet(:name => 'Back To Back Champions')
  date_format = Spreadsheet::Format.new :number_format => 'MM/DD/YYYY'
  sheet.column(1).default_format = date_format
  sheet.column(2).default_format = date_format
  sheet.row(0).push 'Time Frame', 'Start Date', 'End Date', 'Player', 'Number of Weeks'
  count = 1

  data = @raw_data_table.order(:game_date).select_group(:game_date, :big_cash).exclude(big_cash: nil).all

  back_to_back_champions = calc_back_to_back_champions(data)

  back_to_back_champions.each do |row|
    sheet.row(count).push 'All Data', row[:start_date], row[:end_date], row[:name], (((row[:end_date]-row[:start_date])/7).to_i)+1
    count = count+1
  end

end

def calc_back_to_back_champions(data)
  weekly_champions = data.sort { |champion_1, champion_2| champion_1[:game_date] <=> champion_2[:game_date] }

  back_to_back_champions = Array.new
  this_person_won_on_successive_weeks = false
  last_week = weekly_champions.shift

  weekly_champions.each do |this_week|
    if last_week[:big_cash] == this_week[:big_cash]
      if this_person_won_on_successive_weeks
        back_to_back_champions.last[:end_date] = this_week[:game_date]

      else
        back_to_back_champions.push({start_date: last_week[:game_date],
                                     end_date: this_week[:game_date],
                                     name: this_week[:big_cash]})
      end

      this_person_won_on_successive_weeks = true

    else
      this_person_won_on_successive_weeks = false
      last_week = this_week
    end
  end

  back_to_back_champions
end

def create_days_since_last_big_cash
  sheet = $target_workbook.create_worksheet(:name => 'Last Big Cash')
  date_format = Spreadsheet::Format.new :number_format => 'MM/DD/YYYY'
  sheet.column(1).default_format = date_format
  sheet.row(0).push 'Player', 'Last Big Cash', 'Days Since Last Big Cash'
  count = 1

  sql = "select big_cash, max(game_date) as last_big_cash,
         julianday((select max(game_date) from raw_data_table where game_on = 'Yes'))-julianday(max(game_date))||' days' as days_since_last_big_cash
          from raw_data_table
          where game_on = 'Yes'
          group by big_cash
          order by 2 desc, 1 asc"
  @database_handle.fetch(sql) do |row|
    sheet.row(count).push row[:big_cash], row[:last_big_cash], row[:days_since_last_big_cash]#, row[:weeks_since_last_played]
    count = count+1
  end

end

def write_target_workbook_out
  $target_workbook.write("#{$data_directory}/output.xls")
end
