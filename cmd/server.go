package cmd

import (
	"KV_Storage"
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"regexp"
	"strings"
	"sync"
	"time"
)

var reg, _ = regexp.Compile(`'.*?'|".*?"|\S+`)

const connInterval = 8

// ExecCmdFunc func for cmd execute
type ExecCmdFunc func(*KV_Storage.KvDB, []string) (string, error)

// ExecCmd exec cmd map
var ExecCmd = make(map[string]ExecCmdFunc)

func addExecCommand(cmd string, cmdFunc ExecCmdFunc) {
	ExecCmd[strings.ToLower(cmd)] = cmdFunc
}

type Server struct {
	db       *KV_Storage.KvDB
	closed   bool
	mu       sync.Mutex
	done     chan struct{}
	listener net.Listener
}

func NewServer(config KV_Storage.Config) (*Server, error) {
	db, err := KV_Storage.Open(config)
	if err != nil {
		return nil, err
	}
	return &Server{db: db, done: make(chan struct{})}, nil
}

// Listen listen the server
func (s *Server) Listen(addr string) {
	var err error
	s.listener, err = net.Listen("tcp", addr) // 启动一个tcp服务监听端口
	if err != nil {
		log.Printf("tcp listen err: %+v\n", err)
		return
	}

	log.Println("KvDB is running, ready to accept connections.")
	for {
		select {
		case <-s.done:
			return
		default:
			conn, err := s.listener.Accept() // 获取客户端的连接
			if err != nil {
				continue
			}
			go s.handleConn(conn) // 启动一个goroutine异步地处理这个连接
		}
	}
}

// Stop stop the server
func (s *Server) Stop() {
	if s.closed {
		return
	}
	s.mu.Lock()
	close(s.done)
	s.closed = true
	s.listener.Close()
	if err := s.db.Close(); err != nil {
		fmt.Printf("close KvDB err: %+v\n", err)
	}
	s.mu.Unlock()
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	for {
		_ = conn.SetReadDeadline(time.Now().Add(time.Hour * connInterval)) // 设置读取的截止时间，即一段时间内没有数据就主动断开连接

		bufReader := bufio.NewReader(conn)
		b := make([]byte, 4)
		_, err := bufReader.Read(b)
		if err != nil {
			if err != io.EOF {
				log.Printf("read cmd size err: %+v\n", err)
			}
			break
		}

		size := binary.BigEndian.Uint32(b[:4])
		if size > 0 {
			data := make([]byte, size)
			_, err := bufReader.Read(data)
			if err != nil {
				log.Printf("read cmd data err: %+v\n", err)
				break
			}

			cmdAndArgs := reg.FindAllString(string(data), -1)   // 获取到命令
			reply := s.handleCmd(cmdAndArgs[0], cmdAndArgs[1:]) // 执行命令
			info := wrapReplyInfo(reply)                        // 返回响应
			_, err = conn.Write(info)
			if err != nil {
				log.Printf("write reply err: %+v\n", err)
			}
		}
	}
}

func (s *Server) handleCmd(cmd string, args []string) (res string) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic when handle the cmd: %+v", r)
		}
	}()

	exec, exist := ExecCmd[strings.ToLower(cmd)]
	if !exist {
		return "command not found"
	}

	if val, err := exec(s.db, args); err != nil {
		res = fmt.Sprintf("err: %+v", err.Error())
	} else {
		res = val
	}
	return
}

func wrapReplyInfo(reply string) []byte {
	b := make([]byte, len(reply)+4)
	binary.BigEndian.PutUint32(b[:4], uint32(len(reply)))
	copy(b[4:], reply)
	return b
}
