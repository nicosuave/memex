import Foundation
import Darwin

/// Proves which same-user process owns a listening socket, using libproc.
/// Credentials and relayed requests are only sent to a listener whose owner is
/// the exact process Memex expects; a squatter on the address never qualifies.
public enum ProcessSockets {
    /// The process holds a listening TCP socket bound to 127.0.0.1:`port`.
    public static func listensOnLoopback(pid: pid_t, port: Int) -> Bool {
        containsSocket(pid: pid) { info in
            guard info.psi.soi_kind == Int32(SOCKINFO_TCP) else { return false }
            let tcp = info.psi.soi_proto.pri_tcp
            let local = tcp.tcpsi_ini
            // insi_lport keeps network byte order in its low 16 bits.
            let boundPort = Int(UInt16(bigEndian: UInt16(truncatingIfNeeded: local.insi_lport)))
            return tcp.tcpsi_state == Int32(TSI_S_LISTEN) && local.insi_vflag & UInt8(INI_IPV4) != 0
                && boundPort == port && local.insi_laddr.ina_46.i46a_addr4.s_addr == in_addr_t(0x7f00_0001).bigEndian
        }
    }

    /// The process holds a listening Unix domain socket bound at `path`.
    public static func listensOnUnixSocket(pid: pid_t, path: String) -> Bool {
        containsSocket(pid: pid) { info in
            guard info.psi.soi_kind == Int32(SOCKINFO_UN), info.psi.soi_options & Int16(SO_ACCEPTCONN) != 0 else { return false }
            var name = info.psi.soi_proto.pri_un.unsi_addr.ua_sun.sun_path
            let capacity = MemoryLayout.size(ofValue: name)
            let bound = withUnsafePointer(to: &name) {
                $0.withMemoryRebound(to: CChar.self, capacity: capacity) { String(cString: $0) }
            }
            return bound == path
        }
    }

    /// The connected peer of a Unix domain socket.
    public static func peerProcess(of descriptor: Int32) -> pid_t? {
        var pid: pid_t = 0
        var length = socklen_t(MemoryLayout<pid_t>.size)
        guard getsockopt(descriptor, SOL_LOCAL, LOCAL_PEERPID, &pid, &length) == 0, pid > 0 else { return nil }
        return pid
    }

    private static func containsSocket(pid: pid_t, _ matches: (socket_fdinfo) -> Bool) -> Bool {
        guard pid > 0 else { return false }
        let stride = MemoryLayout<proc_fdinfo>.stride
        let needed = proc_pidinfo(pid, PROC_PIDLISTFDS, 0, nil, 0)
        guard needed > 0 else { return false }
        // Leave room for descriptors opened between the size query and the listing.
        var descriptors = [proc_fdinfo](repeating: proc_fdinfo(), count: Int(needed) / stride + 32)
        let filled = descriptors.withUnsafeMutableBytes { buffer in
            proc_pidinfo(pid, PROC_PIDLISTFDS, 0, buffer.baseAddress, Int32(buffer.count))
        }
        guard filled > 0 else { return false }
        for descriptor in descriptors.prefix(Int(filled) / stride) where descriptor.proc_fdtype == UInt32(PROX_FDTYPE_SOCKET) {
            var info = socket_fdinfo()
            let size = Int32(MemoryLayout<socket_fdinfo>.size)
            guard proc_pidfdinfo(pid, descriptor.proc_fd, PROC_PIDFDSOCKETINFO, &info, size) == size else { continue }
            if matches(info) { return true }
        }
        return false
    }
}
