puts Time.now
require 'roo'  #Used for unzipping files
require 'spreadsheet'
import sqlite3
require './lib/validate_data_files_functions'

$data_directory = './data_files'
$target_workbook = Spreadsheet::Workbook.new
#current year is the date to start looking at for current year.  Our Fiscal Year is Sept 1 - Aug 31, so all the
#queries will generate two sets of data, 1 for curr year and 1 for all history.  The date it starts looking is the date
#defined below.  This should be December 1st for the current year thru November
$current_year = Date.new(2018,9,13)
$list_of_tables = ['curr_year_raw_data_table', 'prev_year_raw_data_table', 'raw_data_table']
$data_type_string =  "when 'raw_data_table' then 'All Data' when 'prev_year_raw_data_table' then 'Previous Year' else 'Current Year' end as status,"
$outside_of_ua = "'Hoy', 'Doc', 'Uncle Mike', 'Carle', 'Capt.', 'Matty P', 'Nick Engel', 'Digital Hummus', 'Toddles'"

puts Time.now
extract_files_from_zip
puts Time.now
create_database_tables
puts Time.now
load_and_validate_data
puts Time.now
create_raw_data_sheets
puts Time.now
create_attendance_count_sheet
puts Time.now
create_host_count_sheet
puts Time.now
create_big_cash_count_sheet
puts Time.now
create_back_to_back_winners
puts Time.now
create_big_cash_by_attend_percentage_sheet
puts Time.now
create_days_since_last_big_cash
puts Time.now
create_total_hours_played_sheet
puts Time.now
create_responds_least_amount_sheet
puts Time.now
create_average_player_count_by_venue_sheet
puts Time.now
create_games_played_percentage_sheet
puts Time.now
create_number_of_games_in_ua_sheet
puts Time.now
create_horsepower_sheet
puts Time.now
create_distance_from_scioto_sheet
puts Time.now
create_date_last_played_sheet
puts Time.now
create_number_of_divorces_sheet
puts Time.now
create_eat_most_sheet
puts Time.now
create_hoy_fart_sheet
puts Time.now
create_tom_peed_sheet
puts Time.now
create_mike_bluh_sheet
puts Time.now
write_target_workbook_out
puts Time.now
