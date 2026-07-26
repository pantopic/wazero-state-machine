const std = @import("std");
const statemachine = @import("statemachine");

comptime {
    _ = statemachine;
}

var idx: u64 = 0;
var items: u64 = 0;
var sets: u64 = 0;

export fn _start() void {
    statemachine.persistent(open, update, finish, read);
    statemachine.streaming(streamOpen, streamRecv, streamClosed);
    statemachine.watchable(watchOpen, watchClosed);
}

fn open() u64 {
    return 0;
}

fn update(index: u64, cmd: []u8) statemachine.Result {
    items += 1;
    const value = idx;
    idx = index;
    var i: usize = 0;
    while (std.mem.indexOfPos(u8, cmd, i, "test")) |pos| {
        @memcpy(cmd[pos .. pos + 4], "best");
        i = pos + 4;
    }
    return .{ .value = value, .data = cmd };
}

fn finish() void {
    sets += 1;
}

fn read(query: []u8) statemachine.Result {
    if (std.mem.eql(u8, query, "index")) {
        return .{ .value = idx };
    } else if (std.mem.eql(u8, query, "items")) {
        return .{ .value = items };
    } else if (std.mem.eql(u8, query, "sets")) {
        return .{ .value = sets };
    }
    @panic("Unrecognized query");
}

fn streamOpen() void {
    std.debug.print("wasm stream open\n", .{});
}

fn streamRecv(data: []u8) void {
    std.debug.print("wasm stream recv {s}\n", .{data});
    if (std.mem.eql(u8, data, "close")) {
        std.debug.print("wasm stream close start\n", .{});
        statemachine.streamClose();
        std.debug.print("wasm stream close complete\n", .{});
    } else {
        std.debug.print("wasm stream send start\n", .{});
        statemachine.streamSend(1, data);
        std.debug.print("wasm stream send complete\n", .{});
    }
}

fn streamClosed() void {
    std.debug.print("wasm stream closed\n", .{});
}

fn watchOpen(data: []u8) void {
    std.debug.print("wasm watch open\n", .{});
    const n = std.fmt.parseInt(u64, data, 10) catch @panic("Invalid watch count");
    var i: u64 = 1;
    var tmp: [20]u8 = undefined;
    while (i <= n) : (i += 1) {
        const s = std.fmt.bufPrint(&tmp, "{d}", .{i}) catch unreachable;
        std.debug.print("wasm watch send {s}\n", .{s});
        statemachine.watchSend(1, s);
    }
    statemachine.watchClose();
}

fn watchClosed() void {
    std.debug.print("wasm watch closed\n", .{});
}
