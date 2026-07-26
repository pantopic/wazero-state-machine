const std = @import("std");

const flag_persistent: u16 = 1 << 0;
const flag_streamable: u16 = 1 << 1;
const flag_watchable: u16 = 1 << 2;

pub const Result = struct {
    value: u64 = 0,
    data: []const u8 = &.{},
};

pub const OpenFn = *const fn () u64;
pub const UpdateFn = *const fn (index: u64, cmd: []u8) Result;
pub const FinishFn = *const fn () void;
pub const ReadFn = *const fn (query: []u8) Result;
pub const StreamOpenFn = *const fn () void;
pub const StreamRecvFn = *const fn (cmd: []u8) void;
pub const StreamClosedFn = *const fn () void;
pub const WatchOpenFn = *const fn (cmd: []u8) void;
pub const WatchClosedFn = *const fn () void;

const buf_size: u32 = 2 * 1024 * 1024;

var meta: [8]u32 = undefined;
var flags: u16 = 0;
pub var shard_id: u64 = 0;
pub var replica_id: u64 = 0;
var index: u64 = 0;
var value: u64 = 0;
var buf_cap: u32 = buf_size;
var buf_len: u32 = 0;
var buf: [buf_size]u8 = undefined;

var fn_open: ?OpenFn = null;
var fn_update: ?UpdateFn = null;
var fn_finish: ?FinishFn = null;
var fn_read: ?ReadFn = null;
var fn_stream_open: ?StreamOpenFn = null;
var fn_stream_recv: ?StreamRecvFn = null;
var fn_stream_closed: ?StreamClosedFn = null;
var fn_watch_open: ?WatchOpenFn = null;
var fn_watch_closed: ?WatchClosedFn = null;

pub fn concurrent(update: UpdateFn, finish: FinishFn, read: ReadFn) void {
    fn_update = update;
    fn_finish = finish;
    fn_read = read;
}

pub fn persistent(open: OpenFn, update: UpdateFn, finish: FinishFn, read: ReadFn) void {
    fn_open = open;
    fn_update = update;
    fn_finish = finish;
    fn_read = read;
    flags |= flag_persistent;
}

pub fn streaming(streamOpen: StreamOpenFn, streamRecv: StreamRecvFn, streamClosed: StreamClosedFn) void {
    fn_stream_open = streamOpen;
    fn_stream_recv = streamRecv;
    fn_stream_closed = streamClosed;
    flags |= flag_streamable;
}

pub fn streamSend(val: u64, data: []const u8) void {
    setValue(val);
    setData(data);
    __state_machine_stream_send();
}

pub fn streamClose() void {
    __state_machine_stream_close();
}

pub fn watchable(watchOpen: WatchOpenFn, watchClosed: WatchClosedFn) void {
    fn_watch_open = watchOpen;
    fn_watch_closed = watchClosed;
    flags |= flag_watchable;
}

pub fn watchSend(val: u64, data: []const u8) void {
    setValue(val);
    setData(data);
    __state_machine_watch_send();
}

pub fn watchClose() void {
    __state_machine_watch_close();
}

export fn __state_machine() u32 {
    meta[0] = @intFromPtr(&flags);
    meta[1] = @intFromPtr(&shard_id);
    meta[2] = @intFromPtr(&replica_id);
    meta[3] = @intFromPtr(&index);
    meta[4] = @intFromPtr(&value);
    meta[5] = @intFromPtr(&buf_cap);
    meta[6] = @intFromPtr(&buf_len);
    meta[7] = @intFromPtr(&buf[0]);
    return @intFromPtr(&meta[0]);
}

export fn __state_machine_open() u64 {
    return fn_open.?();
}

export fn __state_machine_update() void {
    const res = fn_update.?(index, buf[0..buf_len]);
    value = res.value;
    setData(res.data);
}

export fn __state_machine_finish() void {
    fn_finish.?();
}

export fn __state_machine_read() void {
    const res = fn_read.?(buf[0..buf_len]);
    value = res.value;
    setData(res.data);
}

export fn __state_machine_stream_open() void {
    if (fn_stream_open) |f| f();
}

export fn __state_machine_stream_recv() void {
    fn_stream_recv.?(buf[0..buf_len]);
}

export fn __state_machine_stream_closed() void {
    if (fn_stream_closed) |f| f();
}

export fn __state_machine_watch_open() void {
    fn_watch_open.?(buf[0..buf_len]);
}

export fn __state_machine_watch_closed() void {
    if (fn_watch_closed) |f| f();
}

fn setData(v: []const u8) void {
    std.mem.copyForwards(u8, buf[0..v.len], v);
    buf_len = @intCast(v.len);
}

fn setValue(v: u64) void {
    value = v;
}

extern "pantopic/wazero-state-machine" fn __state_machine_stream_send() void;
extern "pantopic/wazero-state-machine" fn __state_machine_stream_close() void;
extern "pantopic/wazero-state-machine" fn __state_machine_watch_send() void;
extern "pantopic/wazero-state-machine" fn __state_machine_watch_close() void;
