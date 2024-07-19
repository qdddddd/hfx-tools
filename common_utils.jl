using Dates, DataFramesMeta
using DfUtils

function get_date_symbol(fname::String)
    tup = split(split(fname, "/")[end], "_")
    (String(tup[2][1:8]), String(tup[1]))
end

get_in_file(args, dt) = joinpath(args.task.idir, dt, "$(args.task.symbol)_$(dt).csv.gz")

function get_out_file(args, dt)
    if args.one_file
        if isnothing(args.snapshot_time)
            return joinpath(args.out_root, "$(args.task.symbol).csv$(args.compress ? ".gz" : "")")
        else
            return joinpath(args.out_root, "$(dt).csv$(args.compress ? ".gz" : "")")
        end
    else
        return joinpath(args.out_root, dt, "$(args.task.symbol)_$(dt).csv$(args.compress ? ".gz" : "")")
    end
end

unix2datetime_adj(x) = unix2datetime(x / 1e6) + Hour(8)

##
function read_input_df(date, args)
    ifile = get_in_file(args, date)
    df_raw = CSV.File(ifile; ntasks=1, types=Dict(:Timestamp => Int, :AppSeq => Int)) |> DataFrame

    if !isnothing(args.hdf_root)
        X, _, Y = from_hdf(
            joinpath(args.hdf_root, date, "$(args.task.symbol)_$(date).hdf"),
            feature_names=args.hdf_features, label_names=args.hdf_labels, insert_ts=false
        )
        select!(X, [:AppSeq, :CumAmount, :CumVolume, :CumBuyTurnover, :CumSellTurnover])
        select!(Y, [:AppSeq, :FirstBidPrice, :FirstAskPrice])
        unique!(X, :AppSeq)
        unique!(Y, :AppSeq)
        leftjoin!(df_raw, X, on=:AppSeq)
        leftjoin!(df_raw, Y, on=:AppSeq)
        dropmissing!(df_raw)
    end

    df_raw
end
##

function preprocess_raw!(df, symbol)
    @rtransform!(df, :Timestamp = unix2datetime_adj(:Timestamp))
    @rtransform!(df, :IntradayTime = Time((hour(:Timestamp) > 12 ? :Timestamp - Minute(90) : :Timestamp) - Hour(9) - Minute(30)))
    insertcols!(df, 3, :Code => symbol, :Date => Date.(df.Timestamp))
end
