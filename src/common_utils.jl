using Dates, JlUtils
using .DfUtils

function get_date_symbol(fname::String)
    tup = split(split(fname, "/")[end], "_")
    (String(tup[2][1:8]), String(tup[1]))
end

function get_in_file(args, dt)
    symbol = args.task.symbol == "302132.SZ" && dt <= "20250214" ? "300114.SZ" : args.task.symbol
    joinpath(args.task.idir, dt, "$(symbol)_$(dt).csv.gz")
end

function get_out_file(args, dt)
    out_root = args.use_tmp ? args.out_root_tmp : args.out_root
    if args.one_file
        if isnothing(args.snapshot_time)
            return joinpath(out_root, "$(args.task.symbol).csv$(args.compress ? ".gz" : "")")
        else
            return joinpath(out_root, "$(dt).csv$(args.compress ? ".gz" : "")")
        end
    else
        return joinpath(out_root, dt, "$(args.task.symbol)_$(dt).csv$(args.compress ? ".gz" : "")")
    end
end

unix2datetime_adj(x) = eltype(x) <: Int ? unix2datetime(x / 1e6) + Hour(8) : x

##
function read_input_df(date, args)
    ifile = get_in_file(args, date)
    df_raw = CSV.File(ifile; ntasks=1, types=Dict(:Timestamp => Int, :AppSeq => Int)) |> DataFrame

    if !isnothing(args.hdf_root)
        X, _, Y = from_hdf(
            joinpath(args.hdf_root, date, "$(args.task.symbol)_$(date).hdf"),
            feature_names=args.hdf_features, label_names=args.hdf_labels, insert_ts=false
        )
        if !isnothing(X)
            select!(X, [:AppSeq, :CumAmount, :CumVolume, :CumBuyTurnover, :CumSellTurnover])
            unique!(X, :AppSeq)
            leftjoin!(df_raw, X, on=:AppSeq)
        end

        if !isnothing(Y)
            select!(Y, [:AppSeq, :FirstBidPrice, :FirstAskPrice])
            unique!(Y, :AppSeq)
            leftjoin!(df_raw, Y, on=:AppSeq)
        end
        dropmissing!(df_raw)
    end

    df_raw
end
##

_get_intraday_time(dt::Vector{DateTime}) = Time.(ifelse.(hour.(dt) .> 12, dt .- Minute(90), dt) .- Hour(9) .- Minute(30))

function preprocess_raw!(df, symbol)
    df[!, :Timestamp] = unix2datetime_adj.(df.Timestamp)
    df[!, :IntradayTime] = _get_intraday_time(df.Timestamp)
    insertcols!(df, 3, :Code => symbol, :Date => Date.(df.Timestamp))
end

function get_intraday_time!(args, df)
    dt = unix2datetime_adj.(df.Timestamp)
    args.dt_cols = DataFrame(:Date => Date.(dt), :IntradayTime => _get_intraday_time(dt))
end
