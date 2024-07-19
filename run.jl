using Pkg
Pkg.precompile()

using Base: Semaphore, acquire
using .Threads, CSV, ProgressMeter, Printf, Glob, DataFrames, DataStructures, Dates, GZip, Logging, JSON
using DfUtils, DfUtils

# ENV["JULIA_DEBUG"] = "Main"
@info "Using $(nthreads()) threads"

include("structs.jl")
include("common_utils.jl")
include("process_functions.jl")

const UNPROCESSED = 0x0
const PROCESSING = 0x1
const COMPLETED = 0x2

##
function process(args::ProcessArgs, file_lock::LockedDict{String, ReentrantLock})
    date = args.dates[args.task.dtid]
    ofile = get_out_file(args, date)

    # Check for existing out-file
    if !args.one_file && args.skip_existing && isfile(ofile)
        return
    end

    q_out = args.out_store[args.task.symbol]
    df_raw = read_input_df(date, args)

    # Call user-defined function
    args = ProcessArgs(args; df_raw=df_raw, date=date, out_q=q_out, hfx_cols=r"HFX\d+")
    df_out = args.func(args)

    # Check if function returned None
    if df_out === nothing || isempty(df_out)
        return
    end

    # Round data (assuming numerical)
    df_out = round_df!(df_out, digits=3)

    # Handle dry run
    if args.dry_run
        return
    end

    # Store output
    if args.one_file
        if isnothing(args.snapshot_time)
            CSV.write(ofile, df_out, compress=args.compress, append=isfile(ofile))
        else
            if (!haskey(file_lock, ofile))
                file_lock[ofile] = ReentrantLock()
            end

            lock(file_lock[ofile]) do
                CSV.write(ofile, df_out, compress=args.compress, append=isfile(ofile))
            end
        end
    else
        out_dir = dirname(ofile)
        if !isdir(out_dir)
            mkpath(out_dir)
        end

        if isfile(ofile)
            rm(ofile)
        end

        CSV.write(ofile, df_out, compress=args.compress)
    end

    if !isempty(q_out)
        if endswith(string(nameof(args.func)), "cont")
            @warn "Deque not empty for symbol: $(args.task.symbol)@$(date)"
        end
        empty!(q_out)
    end
    push!(q_out, df_out)

    df_raw = nothing
end

##

function main()
    ## Init params
    # Parse arguments
    start_dt = nothing
    end_dt = nothing
    hfx_model_name = "N8000W100Ver2.2.1SZ"
    d = "/minio/hrmspro/prediction/InfinityRound2/HFX"
    time_thres = 0
    shift = 0
    hdf_root = nothing
    hdf_features = nothing
    hdf_labels = nothing
    stock_info = nothing
    output_root = nothing
    func = corr_downsample
    compress = true
    corr_thres = nothing
    dry_run = false
    skip_existing = false
    clear = false
    one_file = false
    snapshot_time = nothing
    ##

    for i in eachindex(ARGS)
        arg = ARGS[i]
        if arg == "--corr"
            corr_thres = parse(Float32, ARGS[i+1])
        elseif arg == "-s"
            start_dt = ARGS[i+1]
        elseif arg == "-e"
            end_dt = ARGS[i+1]
        elseif arg == "-m"
            hfx_model_name = ARGS[i+1]
        elseif arg == "-d"
            d = ARGS[i+1]
        elseif arg == "-o"
            output_root = ARGS[i+1]
        elseif arg == "-t"
            snapshot_time = ARGS[i+1]
        elseif arg == "--tt"
            time_thres = parse(Int, ARGS[i+1])
        elseif arg == "--no-compress"
            compress = false
        elseif arg == "--skip-existing"
            skip_existing = true
        elseif arg == "--shift"
            shift = parse(Int, ARGS[i+1])
        elseif arg == "--hdf"
            hdf_root = ARGS[i+1]
            schema = JSON.parsefile(joinpath(hdf_root, "description.json"))
            hdf_features = Symbol.(schema["FeatureNames"])
            hdf_labels = Symbol.(schema["LabelNames"])
            stock_info = CSV.read(
                "/ivohf/hrmspro/prediction/InfinityRound2/stock_info.csv", DataFrame;
                types=Dict(:Code => String, :Date => Date), dateformat="yyyymmdd"
            )
        elseif arg == "--func"
            func = getfield(Main, Symbol(ARGS[i+1]))
        elseif arg == "--dry"
            dry_run = true
        elseif arg == "--clear"
            clear = true
        elseif arg == "--one-file"
            one_file = true
        end
    end

    ## Generate params
    if isnothing(hfx_model_name) && !isnothing(hdf_root)
        hfx_model_name = split(hdf_root, ",")[1]
    end

    if isnothing(hfx_model_name)
        throw(ArgumentError("Must provide either -m or --hdf"))
    end

    raw_predict_dir = joinpath(d, hfx_model_name)

    if isnothing(output_root) && !isnothing(corr_thres)
        postfix = endswith(string(nameof(func)), "cont") ? "Cont" : ""
        postfix = endswith(string(nameof(func)), "rev") ? "Rev" : postfix
        output_root = "$(raw_predict_dir)(CorrSelected$(postfix)$(@sprintf("%.2f", corr_thres))$(shift > 0 ? "_$(shift)" : "")$(time_thres > 0 ? "_$(time_thres)s" : ""))"
        if !isnothing(snapshot_time)
            output_root = joinpath(output_root, snapshot_time)
        end
    end

    ##

    if isdir(output_root)
        if clear
            @showprogress desc = "Deleting old files" showspeed = true barlen = 60 @threads for dir in readdir(output_root)
                rm(joinpath(output_root, dir); force=true, recursive=true)
            end
        elseif one_file && skip_existing # Find the true starting date based on existing files
            existing_dt = []
            lk = ReentrantLock()
            @showprogress desc = "Scanning existing files" showspeed = true barlen = 60 @threads for file in readdir(output_root)
                lines = eachline(GZip.open(joinpath(output_root, file)))
                header = first(lines)
                line = nothing
                for l in lines
                    line = l
                end

                if line === nothing
                    continue
                end

                line = header * "\n" * line * "\n"

                lock(lk) do
                    push!(existing_dt, line)
                end
            end

            fmt = "yyyymmdd"
            max_date = maximum(DataFrame(CSV.File(
                map(IOBuffer, existing_dt); select=["Date"], types=Dict(:Date => Date), dateformat=fmt
            )).Date)
            start_dt = Dates.format(max_date + Dates.Day(1), fmt)
        end
    else
        mkpath(output_root)
    end

    ## Generate GARGS
    # Get list of dates
    dates = readdir(raw_predict_dir)
    filter!(x -> occursin(r"^\d{8}$", x), dates)
    sort!(dates)

    st_id = 1
    ed_id = size(dates, 1)

    if start_dt !== nothing
        st_id = findfirst(x -> x >= start_dt, dates)
    end

    if end_dt !== nothing
        ed_id = findlast(x -> x <= end_dt, dates)
    end

    @info "Params" date = "$(dates[st_id]) - $(dates[ed_id])" input_dir = raw_predict_dir output_dir = output_root one_file hdf_root dry_run clear skip_existing

    codes = select_df(
        ClickHouseClient(),
        "
            SELECT DISTINCT S_INFO_WINDCODE Code
            FROM winddb_mirror.ashareeodprices
            WHERE TRADE_DT >= '$(dates[st_id])' AND TRADE_DT <= '$(dates[ed_id])'
        "
    ).Code

    file_lock = LockedDict{String,ReentrantLock}()

    GARGS = ProcessArgs(
        task_dict=Dict{String,LockedDeque{ProcessTask}}([c => LockedDeque{ProcessTask}() for c in codes]),
        out_store=Dict{String,Deque{DataFrame}}([c => Deque{DataFrame}() for c in codes]),
        dfs_buffer=Dict{String,DataFrame}([c => DataFrame() for c in codes]),
        dfs_buffer_sizes=Dict{String,Queue{Int}}([c => Queue{Int}() for c in codes]),
        stock_info=stock_info,
        dates=dates,
        out_root=output_root,
        corr_thres=corr_thres,
        time_thres=Second(time_thres),
        shift=shift,
        hdf_root=hdf_root,
        hdf_features=hdf_features,
        hdf_labels=hdf_labels,
        func=func,
        compress=compress,
        dry_run=dry_run,
        skip_existing=skip_existing,
        one_file=one_file,
        snapshot_time=(
            isnothing(snapshot_time)
            ? nothing
            : Time(parse(Int, snapshot_time[1:2]), parse(Int, snapshot_time[3:4]), 0)
        ),
        roll_win=50,
        n_steps=20,
    )
    term_signal = "#"
    ##

    total_files = Atomic{Int}(0)
    pbar = Progress(total_files[]; desc="$(nameof(func)):", showspeed=true, barlen=60)

    n_symbols_started = Atomic{Int}(0)
    supplier = @spawn begin
        valid_symbols = Set()

        for i in st_id:ed_id
            date = dates[i]
            files = glob("*.csv*", joinpath(raw_predict_dir, date))
            count = 0

            for file in files
                _, symbol = get_date_symbol(file)
                new_task = ProcessTask(i, symbol, raw_predict_dir, UNPROCESSED)

                if !(symbol in valid_symbols)
                    atomic_add!(n_symbols_started, 1)
                    push!(valid_symbols, symbol)
                end

                q = GARGS.task_dict[symbol]
                push!(q, new_task)
                count += 1
            end

            atomic_add!(total_files, count)
            pbar.n = total_files[]
            yield()
        end

        for symbol in valid_symbols
            push!(GARGS.task_dict[symbol], ProcessTask(term_signal, COMPLETED))
        end
    end

    while n_symbols_started[] == 0 && !istaskdone(supplier)
        sleep(0.5)
    end

    if istaskfailed(supplier)
        wait(supplier)
        return
    end

    @info "Start processing tasks..."
    sem = Semaphore(nthreads())
    force_quit = Atomic{Bool}(false)
    n_symbols_terminated = Atomic{Int}(0)

    while n_symbols_terminated[] < n_symbols_started[] || !istaskdone(supplier)
        for symbol in keys(GARGS.task_dict)
            if force_quit[]
                return
            end

            task_q = GARGS.task_dict[symbol]

            if isempty(task_q)
                continue
            end

            task = first(task_q)

            if task.symbol == term_signal
                empty!(GARGS.task_dict[symbol])
                empty!(GARGS.dfs_buffer[symbol])
                empty!(GARGS.out_store[symbol])
                atomic_add!(n_symbols_terminated, 1)
                continue
            end

            if task.status[] == PROCESSING
                continue
            end

            if task.status[] == COMPLETED
                popfirst!(task_q)
                next!(pbar,
                    showvalues=[
                        (:Progress, "$(pbar.counter+1)/$(pbar.n)"),
                        (:NSymbols, "$(n_symbols_terminated[])/$(n_symbols_started[])")
                    ]
                )
                continue
            end

            atomic_xchg!(task.status, PROCESSING)

            @spawn begin
                acquire(sem) do
                    try
                        process(ProcessArgs(GARGS; task=task), file_lock)
                    catch e
                        @error "\nException thrown with $(task.symbol)@$(dates[task.dtid])" exception = (e, catch_backtrace())
                        atomic_xchg!(force_quit, true)
                    end

                    atomic_xchg!(task.status, COMPLETED)
                end
            end
        end

        yield()
    end

    wait(supplier)
    finish!(pbar)
end

main()
