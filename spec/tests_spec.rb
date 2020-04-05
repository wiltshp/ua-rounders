require 'date'
require "./lib/validate_data_files_functions"

describe 'Big Cash Back to Back Champion' do
  it 'will return nil if there are no back to back champions' do
    input_data = [
        {:game_date => Date.new(2016, 1, 1), :big_cash => "PT"},
        {:game_date => Date.new(2016, 1, 8), :big_cash => "Cakes"},
        {:game_date => Date.new(2016, 1, 15), :big_cash => "Uncle Mike"},
        {:game_date => Date.new(2016, 1, 22), :big_cash => "Cakes"},
        {:game_date => Date.new(2016, 1, 29), :big_cash => "Hoy"},
    ]

    result = calc_back_to_back_champions(input_data)

    expect(result).to eq([])
  end

  it 'will return the first date, the last date, and the big_cash if there is a back to back champion' do
    input_data = [
        {:game_date => Date.new(2016, 1, 1), :big_cash => "PT"},
        {:game_date => Date.new(2016, 1, 8), :big_cash => "Uncle Mike"},
        {:game_date => Date.new(2016, 1, 15), :big_cash => "Cakes"},
        {:game_date => Date.new(2016, 1, 22), :big_cash => "Cakes"},
        {:game_date => Date.new(2016, 1, 29), :big_cash => "Hoy"},
    ]

    result = calc_back_to_back_champions(input_data)

    expect(result).to eq([{start_date: Date.new(2016, 1, 15), end_date: Date.new(2016, 1, 22), name: 'Cakes'}])

    input_data = [
        {:game_date => Date.new(2016, 1, 1), :big_cash => "PT"},
        {:game_date => Date.new(2016, 1, 8), :big_cash => "Uncle Mike"},
        {:game_date => Date.new(2016, 1, 15), :big_cash => "Cakes"},
        {:game_date => Date.new(2016, 1, 22), :big_cash => "Hoy"},
        {:game_date => Date.new(2016, 1, 29), :big_cash => "Hoy"},
    ]

    result = calc_back_to_back_champions(input_data)

    expect(result).to eq([{start_date: Date.new(2016, 1, 22), end_date: Date.new(2016, 1, 29), name: 'Hoy'}])
  end

  it 'will return the first date, the last date, and the big_cash in date order if there is more than one back to back champion' do
    input_data = [
        {:game_date => Date.new(2016, 1, 1), :big_cash => "PT"},
        {:game_date => Date.new(2016, 1, 8), :big_cash => "PT"},
        {:game_date => Date.new(2016, 1, 15), :big_cash => "Cakes"},
        {:game_date => Date.new(2016, 1, 22), :big_cash => "Cakes"},
        {:game_date => Date.new(2016, 1, 29), :big_cash => "Hoy"},
    ]

    result = calc_back_to_back_champions(input_data)

    expect(result).to eq([{start_date: Date.new(2016, 1, 1), end_date: Date.new(2016, 1, 8), name: 'PT'}, {start_date: Date.new(2016, 1, 15), end_date: Date.new(2016, 1, 22), name:'Cakes'}])
  end

  it 'will return the first date, the last date, and the big_cash if there is a back to back to back champion' do
    input_data = [
        {:game_date => Date.new(2016, 1, 1), :big_cash => "PT"},
        {:game_date => Date.new(2016, 1, 8), :big_cash => "Cakes"},
        {:game_date => Date.new(2016, 1, 15), :big_cash => "Cakes"},
        {:game_date => Date.new(2016, 1, 22), :big_cash => "Cakes"},
        {:game_date => Date.new(2016, 1, 29), :big_cash => "Hoy"},
    ]

    result = calc_back_to_back_champions(input_data)

    expect(result).to eq([{start_date: Date.new(2016, 1, 8), end_date: Date.new(2016, 1, 22), name: 'Cakes'}])
  end

  it 'will not count a back to back champion if there is a game missed between weeks' do
    pending("Determine what to do around missing games.  Should a missed game mean a missed back to back?")
    input_data = [
        {:game_date => Date.new(2016, 1, 1), :big_cash => "PT"},
        {:game_date => Date.new(2016, 1, 15), :big_cash => "PT"},
        {:game_date => Date.new(2016, 1, 29), :big_cash => "PT"},
    ]

    result = calc_back_to_back_champions(input_data)

    expect(result).to eq([])
  end
end