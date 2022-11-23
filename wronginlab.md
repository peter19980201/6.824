```c
Test: unreliable net, restarts, many clients (3A) ...
info: wrote history visualization to /tmp/3856943182.html
--- FAIL: TestPersistConcurrentUnreliable3A (40.99s)
    test_test.go:293: get wrong value, key 1, wanted:
        x 1 0 yx 1 1 yx 1 2 yx 1 3 yx 1 4 yx 1 5 yx 1 6 yx 1 7 yx 1 8 yx 1 9 yx 1 10 yx 1 11 yx 1 12 yx 1 13 yx 1 14 y
        , got
        x 0 0 yx 0 1 yx 0 2 yx 0 3 yx 0 4 yx 0 5 yx 0 6 yx 0 7 yx 0 8 yx 0 9 yx 0 10 yx 0 11 yx 0 12 yx 0 13 yx 0 14 yx 0 15 yx 0 16 yx 0 17 yx 0 18 yx 0 19 yx 0 20 yx 0 21 yx 0 22 y
    test_test.go:126: failure
    test_test.go:382: history is not linearizable
```

1. client发送get请求给leader，leader传送给raft，获得一个index，此时leader离线
2. raft选举出新的leader，新leader接收到了一个新的get请求，获得了一个新的index，与上边的index相同
3. 原来的leader收到这条更新日志，将数据发送给原来的通道，再发送给原来的client
4. 在应用新apply的地方对自己是否是leader进行判断

```c
//在restart client测试中出现这种情况
	if kv.replyChMap[applyMsg.CommandIndex] == nil {
		return
	}
```

出现场景：切换leader后，新leader收到的op，但是raft没有在500ms内给出响应，导致超时，client重新发送。多条相同的op在第一条取得响应后，通道关闭，后面的掉入这个if中

应该是属于底层raft的问题