using Statistics, DfUtils, DbUtils, DataFramesMeta

function _corr_downsample(args::ProcessArgs, hfx::Matrix; return_corr::Bool=false)
    ind = args.shift + 1

    if ind >= size(hfx, 1)
        return nothing, nothing
    end

    subset_ind = [ind]
    subset_corr = return_corr ? [args.corr_thres] : nothing

    while ind <= size(hfx, 1)
        cur = ind + 1
        while cur <= size(hfx, 1)
            corr = cor((@view hfx[ind, :]), (@view hfx[cur, :]))

            if ismissing(corr) || corr >= args.corr_thres
                cur += 1
                continue
            end

            cur += args.shift

            time_diff = Millisecond(args.dt_cols[cur, :Date] == args.dt_cols[ind, :Date]
                                    ? args.dt_cols[cur, :IntradayTime] - args.dt_cols[ind, :IntradayTime]
                                    : args.dt_cols[cur, :IntradayTime] + Hour(4) - args.dt_cols[ind, :IntradayTime]
            )

            if ind <= size(hfx, 1) && time_diff >= args.time_thres
                push!(subset_ind, cur)
                if !isnothing(subset_corr)
                    push!(subset_corr, corr)
                end
            end

            ind = cur

            break
        end

        if cur > size(hfx, 1)
            break
        end
    end

    return subset_ind, subset_corr
end

function _corr_downsample_rev(args::ProcessArgs, hfx::Matrix; return_corr::Bool=false)
    ind = size(hfx, 1) - args.shift

    if ind <= 1
        return nothing, nothing
    end

    subset_ind = [ind]
    subset_corr = return_corr ? [args.corr_thres] : nothing

    n_total = isnothing(args.snapshot_time) ? typemax(Int) : args.n_steps + args.roll_win

    while ind >= 1
        cur = ind - 1
        while cur >= 1
            corr = cor((@view hfx[ind, :]), (@view hfx[cur, :]))

            if ismissing(corr) || corr >= args.corr_thres
                cur -= 1
                continue
            end

            cur -= args.shift

            time_diff = Millisecond(args.dt_cols[cur, :Date] == args.dt_cols[ind, :Date]
                                    ? args.dt_cols[ind, :IntradayTime] - args.dt_cols[cur, :IntradayTime]
                                    : args.dt_cols[ind, :IntradayTime] + Hour(4) - args.dt_cols[cur, :IntradayTime]
            )

            if cur >= 1 && time_diff >= args.time_thres
                push!(subset_ind, cur)
                if !isnothing(subset_corr)
                    push!(subset_corr, corr)
                end
            end

            ind = cur

            break
        end

        if cur < 1 || length(subset_ind) >= n_total
            break
        end
    end

    return subset_ind, subset_corr
end

function corr_downsample(args::ProcessArgs)
    hfx = Matrix(args.df_raw[!, args.hfx_cols])
    get_intraday_time!(args, args.df_raw)
    subset_ind, _ = _corr_downsample(args, hfx)

    if subset_ind === nothing
        return nothing
    end

    df_subset = args.df_raw[subset_ind, :]
    args.df_raw = nothing
    return df_subset
end

function corr_downsample_cont(args::ProcessArgs)
    input_df = nothing
    last_selected_df = nothing

    insertcols!(args.df_raw, 3, :Code => args.task.symbol, :Date => args.dates[args.task.dtid])

    if isempty(args.out_q) && args.task.dtid > 1
        prev_ofile = get_out_file(args, args.dates[args.task.dtid-1])

        if isfile(prev_ofile)
            last_selected_df = CSV.File(prev_ofile, ntasks=1) |> DataFrame
        end
    elseif !isempty(args.out_q)
        last_selected_df = pop!(args.out_q)
    end

    input_df = isnothing(last_selected_df) ? args.df_raw : vcat(last_selected_df[end:end, Not(:Index1000Price)], args.df_raw)
    hfx = Matrix(input_df[!, args.hfx_cols])
    get_intraday_time!(args, input_df)
    subset_ind, _ = _corr_downsample(args, hfx)

    if !isnothing(last_selected_df)
        subset_ind = subset_ind[2:end]
    end

    if isnothing(subset_ind)
        subset_ind = []
    end

    if isempty(subset_ind)
        if !isnothing(last_selected_df)
            push!(args.out_q, last_selected_df)
        end
    end

    df_subset = input_df[subset_ind, :]
    args.df_raw = nothing

    if isempty(df_subset)
        return nothing
    end

    # Add index prices
    stid = findfirst(x -> x >= df_subset.Timestamp[1], args.index_df.ExTime)
    stid = max(1, stid - 1)
    edid = findlast(x -> x <= df_subset.Timestamp[end], args.index_df.ExTime)
    if isnothing(edid)
        df_subset[!, :Index1000Price] .= missing
    else
        sub = @view args.index_df[stid:edid, :]
        df_subset[!, :Index1000Price] = interpolate(sub, df_subset.Timestamp, :LastPrice, :ExTime)
    end

    return df_subset
end

function corr_downsample_rev(args::ProcessArgs)
    preprocess_raw!(args.df_raw, args.task.symbol)
    date = args.df_raw[1, :Date]

    input_df = args.dfs_buffer[args.task.symbol]
    if isempty(input_df) || input_df[end, :Date] < date
        append!(input_df, args.df_raw, promote=true)
    end

    df_raw = @view input_df[input_df.Timestamp.<(date+args.snapshot_time), :]
    if isempty(df_raw)
        return nothing
    end

    last_row = @view df_raw[end:end, :]
    min_date = df_raw[1, :Date]
    min_date_str = Dates.format(min_date, "yyyymmdd")
    hfx = Matrix(df_raw[!, args.hfx_cols])
    args.dt_cols = df_raw[!, [:Date, :IntradayTime]]

    # Fill until reaching the required number of steps
    df_subset = DataFrame()
    dt_cur = args.task.dtid
    n_total = args.n_steps + args.roll_win - 1
    while nrow(df_subset) < n_total && dt_cur > 0
        subset_ind, _ = _corr_downsample_rev(args, hfx)

        if !isnothing(subset_ind) && !isempty(subset_ind)
            append!(df_subset, df_raw[subset_ind[2:end], :], promote=true)
        end

        if nrow(df_subset) >= n_total
            break
        end

        ifile = nothing
        ifile_ok = false

        while dt_cur > 1
            dt_cur -= 1

            if args.dates[dt_cur] >= min_date_str
                continue
            end

            ifile = get_in_file(args, args.dates[dt_cur])
            ifile_ok = isfile(ifile)

            if ifile_ok
                break
            end
        end

        if !ifile_ok
            break
        end

        first_row = df_raw[1:1, :]
        df_raw = read_input_df(args.dates[dt_cur], args)
        preprocess_raw!(df_raw, args.task.symbol)
        args.dfs_buffer[args.task.symbol] = vcat(df_raw, args.dfs_buffer[args.task.symbol])
        append!(df_raw, first_row)
        hfx = Matrix(df_raw[!, args.hfx_cols])
        args.dt_cols = df_raw[!, [:Date, :IntradayTime]]
    end

    if isempty(df_subset)
        return nothing
    end

    append!(df_subset, last_row, cols=:union)
    sort!(df_subset, :Timestamp)
    min_date = df_subset.Date |> minimum

    if !isnothing(args.hdf_root)
        df_subset = gen_infinity_features(args, df_subset)
    end

    len = nrow(df_subset)
    if len < args.n_steps + 1
        empty_rows = DataFrame()
        for _ in (len+1):(args.n_steps+1)
            push!(empty_rows, (Date=Date(0), Code=args.task.symbol), cols=:subset)
        end

        df_subset = append!(empty_rows, df_subset, cols=:union)
    end

    args.df_raw = nothing

    # Update buffer size
    days_rec = args.dfs_buffer_sizes[args.task.symbol]
    subset_days = (date - min_date).value
    enqueue!(days_rec, subset_days)
    while length(days_rec) >= 20
        dequeue!(days_rec)
    end

    dmax = maximum(days_rec)
    df_buffer = args.dfs_buffer[args.task.symbol]
    bdays = (date - df_buffer[1, :Date]).value
    if bdays > dmax
        @rsubset!(df_buffer, :Date >= date - Day(dmax))
    end

    return df_subset
end

function gen_infinity_features(args::ProcessArgs, df)
    info_view = @view args.stock_info[args.stock_info.Code.==args.task.symbol, :]
    leftjoin!(df, info_view, on=[:Date, :Code])

    df[!, :IsOvn] = (df.Date .!= DfUtils.shift(df.Date, 1, df[1, :Date]))
    df[!, :PrevCumBuyTurnover] = DfUtils.shift(df.CumBuyTurnover, 1, 0)
    df[!, :PrevCumSellTurnover] = DfUtils.shift(df.CumSellTurnover, 1, 0)
    df[!, :PrevAskPrice] = DfUtils.shift(df.FirstAskPrice, 1, df[1, :OpenPrice])
    df[!, :PrevBidPrice] = DfUtils.shift(df.FirstBidPrice, 1, df[1, :OpenPrice])
    df[!, :PrevAdjFactor] = DfUtils.shift(df.AdjFactor, 1, df[1, :AdjFactor])
    shifted_time = DfUtils.shift(df.IntradayTime, 1, Time(0))
    df[!, :DeltaT] = ifelse.(df.IsOvn, df.IntradayTime .+ Hour(4) .- shifted_time, df.IntradayTime .- shifted_time)

    df[!, :DeltaBuyTurnover] = ifelse.(df.IsOvn, df.CumBuyTurnover, df.CumBuyTurnover .- df.PrevCumBuyTurnover)
    df[!, :DeltaSellTurnover] = ifelse.(df.IsOvn, df.CumSellTurnover, df.CumSellTurnover .- df.PrevCumSellTurnover)
    df[!, :DeltaTurnover] = df.DeltaBuyTurnover .+ df.DeltaSellTurnover
    df[!, :RollingDeltaTurnover] = rolling_mean(df.DeltaTurnover, args.roll_win, forward=false)

    if nrow(df) > args.n_steps + 1
        df = df[end-args.n_steps:end, :]
    end

    df[!, :DeltaTLog] = log.(Dates.value.(Millisecond.(df.DeltaT)) ./ 1e3)
    df[!, :AvgPrice] = df.CumAmount ./ df.CumVolume
    df[!, :DeltaTurnoverRatio] = df.DeltaTurnover ./ df.RollingDeltaTurnover .- 1
    df[!, :CurrentT] = (df.IntradayTime .- Time(0)) ./ Hour(4) .* 10
    df[!, :DeltaMidRtn] = ((df.FirstBidPrice .+ df.FirstAskPrice) ./ (df.PrevAskPrice .+ df.PrevBidPrice) .* (df.AdjFactor ./ df.PrevAdjFactor) .- 1) .* 1e3
    df[!, :A2NRtn] = ((df.FirstBidPrice .+ df.FirstAskPrice) ./ (2 .* df.AvgPrice) .- 1) .* 1e3 .- df.DeltaMidRtn
    df[!, :DeltaNetTurnoverRatio] = ((df.DeltaBuyTurnover .- df.DeltaSellTurnover) ./ (df.DeltaBuyTurnover .+ df.DeltaSellTurnover)) .* 10
    fillna!(df.DeltaNetTurnoverRatio, 0)

    select!(df, "Date", "Code", "Timestamp", "AppSeq", args.hfx_cols, "Norm", "DeltaTLog", "CurrentT", "A2NRtn", "DeltaMidRtn", "DeltaTurnoverRatio", "DeltaNetTurnoverRatio")
end
