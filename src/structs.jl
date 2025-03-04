import Base.Threads.Atomic
using ConcurrentUtils

## ProcessTask
struct ProcessTask
    dtid::Int
    symbol::AbstractString
    idir::AbstractString
    status::Atomic{UInt8}
end

ProcessTask(dtid::Int, symbol::AbstractString, idir::AbstractString, status::UInt8) = ProcessTask(dtid, symbol, idir, Atomic{UInt8}(status))
ProcessTask(signal::AbstractString, status::UInt8) = ProcessTask(0, signal, "", Atomic{UInt8}(status))

## ProcessArgs
struct ProcessArgs
    kwargs::Dict{Symbol,Any}
end

ProcessArgs(; kwargs...) = ProcessArgs(Dict{Symbol,Any}(kwargs))
ProcessArgs(p::ProcessArgs; kwargs...) = ProcessArgs(Dict{Symbol,Any}(vcat(collect(getfield(p, :kwargs)), collect(kwargs))))

import Base.getproperty, Base.setproperty!
Base.getproperty(x::ProcessArgs, name::Symbol) = getfield(x, :kwargs)[name]
Base.setproperty!(x::ProcessArgs, name::Symbol, v) = getfield(x, :kwargs)[name] = v

## LockedDeque
using DataStructures
struct LockedDeque{T}
    lock::ReentrantLock
    q::Deque{T}
end

LockedDeque{T}() where {T} = LockedDeque(ReentrantLock(), Deque{T}())

import Base.first, Base.last, Base.isempty, Base.empty!, Base.pop!, Base.popfirst!, Base.push!, Base.pushfirst!, Base.length
first(q::LockedDeque) =
    lock(q.lock) do
        first(q.q)
    end
last(q::LockedDeque) =
    lock(q.lock) do
        last(q.q)
    end
isempty(q::LockedDeque) =
    lock(q.lock) do
        isempty(q.q)
    end
empty!(q::LockedDeque) =
    lock(q.lock) do
        empty!(q.q)
    end
pop!(q::LockedDeque) =
    lock(q.lock) do
        pop!(q.q)
    end
popfirst!(q::LockedDeque) =
    lock(q.lock) do
        popfirst!(q.q)
    end
push!(q::LockedDeque, x) =
    lock(q.lock) do
        push!(q.q, x)
    end
pushfirst!(q::LockedDeque, x) =
    lock(q.lock) do
        pushfirst!(q.q, x)
    end
length(q::LockedDeque) =
    lock(q.lock) do
        length(q.q)
    end

## LockedDict
struct LockedDict{K,V}
    lock::ReadWriteLock
    d::Dict{K,V}
end

LockedDict{K,V}() where {K,V} = LockedDict(ReadWriteLock(), Dict{K,V}())

import Base.getindex, Base.setindex!, Base.haskey, Base.keys, Base.delete!
getindex(d::LockedDict{K,V}, k::K) where {K,V} =
    lock_read(d.lock) do
        d.d[k]
    end

setindex!(d::LockedDict{K,V}, v::V, k::K) where {K,V} =
    lock(d.lock) do
        d.d[k] = v
    end

haskey(d::LockedDict{K,V}, k::K) where {K,V} =
    lock_read(d.lock) do
        haskey(d.d, k)
    end

isempty(d::LockedDict{K,V}) where {K,V} =
    lock_read(d.lock) do
        isempty(d.d)
    end

keys(d::LockedDict{K,V}) where {K,V} =
    lock_read(d.lock) do
        keys(d.d)
    end

delete!(d::LockedDict{K,V}, k::K) where {K,V} =
    lock(d.lock) do
        delete!(d.d, k)
    end
