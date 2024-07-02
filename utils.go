package ddsutils

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/892294101/go-mysql/mysql"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"io"
	"io/ioutil"
	"syscall"

	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

const (
	DefaultTimeFormat = "2006-01-02 15:04:05.9999 Z07:00"
)

// 支持的数据库和进程类型
const (
	MySQL      = "MySQL"
	Oracle     = "Oracle"
	Extract    = "EXTRACT"
	Replicat   = "REPLICAT"
	MinRpcPort = 37401
	MaxRpcPort = MinRpcPort + 500
)

// 进程文件和组文件使用
const (
	PROGRAM     = "PROGRAM"
	PROCESSID   = "PROCESSID"
	PORT        = "PORT"
	PID         = "PID"
	DBTYPE      = "DBTYPE"
	PROCESSTYPE = "PROCESSTYPE"
	STATUS      = "STATUS"
	LASTSTARTED = "LAST STARTED"
	FILENUMBER  = "FILE NUMBER"
	LOGNUMBER   = "LOG NUMBER"
	OFFSET      = "OFFSET"
	RUNNING     = "RUNNING"
	STARTING    = "STARTING"
	STOPPING    = "STOPPING"
	STOPPED     = "STOPPED"
	ABENDED     = "ABENDED"
)

// process参数
var (
	ProcessType    = "PROCESS" // 参数类型
	ProcessRegular = "(^)(?i:(" + ProcessType + "))(\\s+)((?:[A-Za-z0-9_]){4,12})($)"
)

// sourcedb参数
var (
	SourceDBType = "SOURCEDB"  // 参数类型
	Port         = "PORT"      // 端口关键字
	DataBase     = "DATABASE"  // 默认连接的数据库
	Types        = "TYPE"      // 库类型,可选mysql mariadb
	UserId       = "USERID"    // 连接用户
	PassWord     = "PASSWORD"  // 连接密码
	ServerId     = "SERVERID"  // mysql server id
	Retry        = "RETRY"     // 连接重试最大
	Character    = "CHARACTER" // 客户端字符集关键字
	Collation    = "COLLATION" // 客户端字符集排序
	TimeZone     = "TIMEZONE"  // 客户端时区

	DefaultPort            uint16 = 3306    // 默认端口
	DefaultDataBase               = "test"  // 默认连接数据库
	DefaultTypes                  = "mysql" // 默认库类型
	DefaultUserId                 = "root"  // 默认用户名
	DefaultMaxRetryConnect        = 3
	DefaultClientCharacter        = "utf8mb4"
	DefaultClientCollation        = "utf8mb4_general_ci"
	DefaultTimeZone, _            = time.LoadLocation("Asia/Shanghai")
)

// traildir 参数
var (
	TrailDirType          = "TRAILDIR" // 参数类型
	TrailSizeKey          = "SIZE"     // size关键字
	TrailKeepKey          = "KEEP"     // keey 关键字
	MB                    = "MB"
	GB                    = "GB"
	DAY                   = "DAY"
	DefaultTrailMaxSize   = 128 // 默认trail文件的最大, 单位是M, 单位不可更改
	DefaultTrailMinSize   = 16  // 默认trail文件的最小
	DefaultTrailKeepValue = 7   // 默认trail文件保留时间,默认是天
)

// discardfile 参数
var (
	DiscardFileType    = "DISCARDFILE"
	DiscardFileRegular = "(^)(?i:(" + DiscardFileType + "))(\\s+)((.+))($)"
)

// dboptions 参数
var (
	DBOptionsType      = "DBOPTIONS"
	SuppressionTrigger = "SUPPRESSIONTRIGGER" // 表操作时抑制触发器
	IgnoreReplicates   = "IGNOREREPLICATES"   // 忽略复制进程执行的操作
	GetReplicates      = "GETREPLICATES"      // 获取复制进程的操作
	IgnoreForeignkey   = "IGNOREFOREIGNKEY"   // 忽略外键约束
)

// TABLE 参数
var (
	TableType    = "TABLE"
	TableRegular = "(^)(?i:(" + TableType + "))(\\s+)((\\S+)(\\.)(\\S+\\s*)(;))($)"
)

// TABLEExclude 参数
var (
	TableExcludeType    = "TABLEEXCLUDE"
	TableExcludeRegular = "(^)(?i:(" + TableExcludeType + "))(\\s+)((\\S+)(\\.)(\\S+\\s*)(;))($)"
)

// UserID参数
var (
	OUserIDType = "USERID"    // 参数类型
	OPort       = "PORT"      // 端口关键字
	OSid        = "SID"       // 端口关键字
	OUser       = "USER"      // 连接用户
	OPassWord   = "PASSWORD"  // 连接密码
	ORetry      = "RETRY"     // 连接重试最大
	OCharacter  = "CHARACTER" // 客户端字符集关键字
	OTimeZone   = "TIMEZONE"  // 客户端字符集关键字

	ODefaultPort            uint16 = 1521 // 默认端口
	ODefaultMaxRetryConnect        = 3
	ODefaultTimeZone               = "Asia/Shanghai"
)

var (
	DDL     = "DDL"
	INCLUDE = "INCLUDE"
	EXCLUDE = "EXCLUDE"
	OPTYPE  = "OPTYPE"
	OBJTYPE = "OBJTYPE"
	OBJNAME = "OBJNAME"

	// 操作类型
	CREATE = "CREATE"
	ALTER  = "ALTER"
	DROP   = "DROP"

	// 对象类型
	TABLE     = "TABLE"
	INDEX     = "INDEX"
	TRIGGER   = "TRIGGER"
	SEQUENCE  = "SEQUENCE"
	VIEW      = "VIEW"
	FUNCTION  = "FUNCTION"
	PACKAGE   = "PACKAGE"
	PROCEDURE = "PROCEDURE"
	TYPE      = "TYPE"
	ROLE      = "ROLE"
	USER      = "USER"
	EVENT     = "EVENT"
	DATABASE  = "DATABASE"
)

// 根据执行文件路径获取程序的HOME路径
func GetHomeDirectory() (s *string, err error) {
	file, _ := exec.LookPath(os.Args[0])
	ExecFilePath, _ := filepath.Abs(file)
	var dir string

	os := runtime.GOOS
	switch os {
	case "windows":
		execfileslice := strings.Split(ExecFilePath, `\`)
		HomeDirectory := execfileslice[:len(execfileslice)-2]
		for i, v := range HomeDirectory {
			if v != "" {
				if i > 0 {
					dir += `\` + v
				} else {
					dir += v
				}
			}
		}
	case "linux", "darwin":
		execfileslice := strings.Split(ExecFilePath, "/")
		HomeDirectory := execfileslice[:len(execfileslice)-2]
		for _, v := range HomeDirectory {
			if v != "" {
				dir += `/` + v
			}
		}
	default:
		return nil, errors.Errorf("Unsupported operating system type: %s", os)
	}

	if dir == "" {
		return nil, errors.Errorf("Get program home directory failed: %s", dir)
	}
	return &dir, nil
}

func HasPrefixIgnoreCase(s, prefix string) bool {
	return len(s) >= len(prefix) && strings.EqualFold(s[0:len(prefix)], prefix)
}

func TrimKeySpace(s []string) []string {
	var deDup []string
	for _, rv := range s {
		if strings.TrimSpace(rv) != "" {
			deDup = append(deDup, strings.TrimSpace(rv))
		}
	}
	return deDup
}

func KeyCheck(s *string) bool {
	key := map[string]string{
		strings.ToUpper(SourceDBType): SourceDBType,
		strings.ToUpper(Port):         Port,
		strings.ToUpper(DataBase):     DataBase,
		strings.ToUpper(Types):        Types,
		strings.ToUpper(UserId):       UserId,
		strings.ToUpper(PassWord):     PassWord,
		strings.ToUpper(TrailDirType): TrailDirType,
		strings.ToUpper(TrailSizeKey): TrailSizeKey,
		strings.ToUpper(TrailKeepKey): TrailKeepKey,
		strings.ToUpper(INCLUDE):      INCLUDE,
		strings.ToUpper(EXCLUDE):      EXCLUDE,
	}
	_, ok := key[strings.ToUpper(*s)]
	return ok
}

// 切片转为字符类型
func SliceToString(kv []string, sp string) *string {
	var kwsb strings.Builder
	var kw string
	if len(kv) > 0 {
		for i, v := range kv {
			if i == 0 {
				kwsb.WriteString(v)
			} else {
				if len(sp) > 0 {
					kwsb.WriteString(sp)
				} else {
					kwsb.WriteString(" ")
				}
				kwsb.WriteString(v)

			}
		}
		kw = kwsb.String()
		return &kw
	}
	return nil
}

func ConvertPositionToNumber(pos *mysql.Position) (s *uint64, p *uint64, err error) {
	if pos == nil {
		return nil, nil, errors.Errorf("Cannot disassemble empty postion information")
	}

	pn := uint64(pos.Pos)

	i := strings.LastIndexByte(pos.Name, '.')
	if i == -1 {
		return nil, nil, errors.Errorf("error parsing position because position format is incorrect: %s", pos.Name)
	}
	seq, err := strconv.Atoi(pos.Name[i+1:])
	fn := uint64(seq)
	if err != nil {
		return nil, nil, errors.Errorf("error parsing position %s. because it does not contain numbers: %s", err, pos.Name)
	}
	return &fn, &pn, nil
}

// 判断文件是否存在
func IsFileExist(path string) bool {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

// 判断文件夹是否存在
func PathExists(path string) bool {
	if fi, err := os.Stat(path); err == nil {
		return fi.IsDir()
	}
	return false
}

// =======================================================================================================
// 自定义Panic异常处理,调用方式: 例如Test()函数, 指定defer ErrorCheckOfRecover(Test)
func GetFunctionName(i interface{}, seps ...rune) string {
	u := runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Entry()
	f, _ := runtime.FuncForPC(reflect.ValueOf(i).Pointer()).FileLine(u)
	return f
}

var GlobalProcessID string

func ErrorCheckOfRecover(n interface{}, log *logrus.Logger) {
	if err := recover(); err != nil {
		home, _ := GetHomeDirectory()
		if home != nil && len(GlobalProcessID) > 0 {
			_ = os.Remove(filepath.Join(*home, "pcs", GlobalProcessID))
		}
		log.Errorf("Panic Message: %s", err)
		log.Errorf("Exception File: %s", GetFunctionName(n))
		log.Errorf("Print Stack Message: %s", string(debug.Stack()))
		log.Fatal("Abnormal exit of program")
	}
}

var (
	IsNotValue = make([]byte, 0)
)

func ConvertColumnValType(data interface{}) ([]byte, error) {
	if data == nil {
		return IsNotValue, nil
	}
	switch v := data.(type) {
	case []byte:
		return v, nil
	case int32:
		b := ConvertPoolGet()
		if err := binary.Write(b, binary.LittleEndian, v); err != nil {
			return nil, err
		}
		r := b.Bytes()
		return r, nil
	case int64:
		b := ConvertPoolGet()
		if err := binary.Write(b, binary.LittleEndian, v); err != nil {
			return nil, err
		}
		r := b.Bytes()
		return r, nil
	case int8:
		b := ConvertPoolGet()
		if err := binary.Write(b, binary.LittleEndian, v); err != nil {
			return nil, err
		}
		r := b.Bytes()
		return r, nil
	case int16:
		b := ConvertPoolGet()
		if err := binary.Write(b, binary.LittleEndian, v); err != nil {
			return nil, err
		}
		r := b.Bytes()
		return r, nil
	case string:
		return *(*[]byte)(unsafe.Pointer(&v)), nil
	case float32:
		b := ConvertPoolGet()
		if err := binary.Write(b, binary.LittleEndian, v); err != nil {
			return nil, err
		}
		r := b.Bytes()
		return r, nil
	case float64:
		b := ConvertPoolGet()
		if err := binary.Write(b, binary.LittleEndian, v); err != nil {
			return nil, err
		}
		r := b.Bytes()
		return r, nil
	case uint32:
		b := ConvertPoolGet()
		if err := binary.Write(b, binary.LittleEndian, v); err != nil {
			return nil, err
		}
		r := b.Bytes()
		return r, nil
	case uint64:
		b := ConvertPoolGet()
		if err := binary.Write(b, binary.LittleEndian, v); err != nil {
			return nil, err
		}
		r := b.Bytes()
		return r, nil
	case time.Time:
		b := ConvertPoolGet()
		if err := binary.Write(b, binary.LittleEndian, v.UnixNano()); err != nil {
			return nil, err
		}
		r := b.Bytes()
		return r, nil
	case int:
		b := ConvertPoolGet()
		if err := binary.Write(b, binary.LittleEndian, int32(v)); err != nil {
			return nil, err
		}
		r := b.Bytes()
		return r, nil
	default:
		return nil, errors.Errorf("Unknown data type while converting type: %v", v)
	}

	/*switch ColumnType {
	case mysql.MYSQL_TYPE_NULL:
	case mysql.MYSQL_TYPE_LONG:
	case mysql.MYSQL_TYPE_TINY:
	case mysql.MYSQL_TYPE_SHORT:
	case mysql.MYSQL_TYPE_INT24:
	case mysql.MYSQL_TYPE_LONGLONG:
	case mysql.MYSQL_TYPE_NEWDECIMAL:
	case mysql.MYSQL_TYPE_FLOAT:
	case mysql.MYSQL_TYPE_DOUBLE:
	case mysql.MYSQL_TYPE_BIT:
	case mysql.MYSQL_TYPE_TIMESTAMP:
	case mysql.MYSQL_TYPE_TIMESTAMP2:
	case mysql.MYSQL_TYPE_DATETIME:
	case mysql.MYSQL_TYPE_DATETIME2:
	case mysql.MYSQL_TYPE_TIME:
	case mysql.MYSQL_TYPE_TIME2:
	case mysql.MYSQL_TYPE_DATE:
	case mysql.MYSQL_TYPE_YEAR:
	case mysql.MYSQL_TYPE_ENUM:
	case mysql.MYSQL_TYPE_SET:
	case mysql.MYSQL_TYPE_BLOB:
	case mysql.MYSQL_TYPE_VARCHAR, mysql.MYSQL_TYPE_VAR_STRING:
	case mysql.MYSQL_TYPE_STRING:
	case mysql.MYSQL_TYPE_JSON:
	case mysql.MYSQL_TYPE_GEOMETRY:
	}*/
}

func GetAvailablePort() (int, error) {
	for i := MinRpcPort; i < MaxRpcPort; i++ {
		add := net.JoinHostPort("127.0.0.1", strconv.Itoa(i))
		d := net.Dialer{Timeout: time.Second * 3}
		conn, err := d.Dial("tcp", add)
		if err != nil {
			_, ok := conn.(*net.TCPConn)

			if !ok {
				return i, nil
			}
		}

	}
	return 0, errors.Errorf("No available ports detected")
}

func NanoSecondConvertToTime(t uint64) string {
	return time.Unix(0, int64(t)).Format(DefaultTimeFormat)
}

// 传入纳秒, 计算事务做检查点的lag耗时
func TimeDifferForCurrentTime(first uint64) string {
	first = first / 1e9
	currentTime := time.Now().Unix()
	resTime := currentTime - int64(first)

	fmt.Printf("first: %v   currentTime: %v  \n", currentTime, resTime)

	var day = resTime / (24 * 3600)
	hour := (resTime - day*3600*24) / 3600
	minute := (resTime - day*24*3600 - hour*3600) / 60
	second := resTime - day*24*3600 - hour*3600 - minute*60

	return fmt.Sprintf("%02d:%02d:%02d:%02d", day, hour, minute, second)
}

// 传入秒, 计算数据流的lag耗时
func DataStreamLagTime(first uint64) string {
	currentTime := time.Now().Unix()
	resTime := currentTime - int64(first)
	var day = resTime / (24 * 3600)
	hour := (resTime - day*3600*24) / 3600
	minute := (resTime - day*24*3600 - hour*3600) / 60
	second := resTime - day*24*3600 - hour*3600 - minute*60
	return fmt.Sprintf("%02d:%02d:%02d:%02d", day, hour, minute, second)
}

func GetAllGroupFileName(pathname string, suffix string) (s []string, e error) {
	rd, err := ioutil.ReadDir(pathname)
	if err != nil {
		return nil, err
	}
	for _, fi := range rd {
		if !fi.IsDir() {
			ok := strings.HasSuffix(fi.Name(), suffix)
			if ok {
				s = append(s, fi.Name())
			}

		}
	}
	return s, nil
}

func GetAllFileFullPath(pathname string, suffix string) (s []string, e error) {
	rd, err := ioutil.ReadDir(pathname)
	if err != nil {
		return nil, err
	}
	var fullName string
	for _, fi := range rd {
		if !fi.IsDir() {
			ok := strings.HasSuffix(fi.Name(), suffix)
			if ok {
				os := runtime.GOOS
				switch os {
				case "windows":
					fullName = pathname + `\` + fi.Name()
				case "linux":
					fullName = pathname + "/" + fi.Name()
				}
				s = append(s, fullName)
			}

		}
	}
	return s, nil
}

type ProcessFile struct {
	File      string // 进程文件名
	PROGRAM   string // 进程类型
	PROCESSID string // 进程名称
	PORT      string // RPC port
	PID       string // 进程PID
	STATUS    string // 运行状态
	DBTYPE    string // 数据库类型
}

// 组ID 文件
type GroupInfo struct {
	GroupID       string
	DbType        string
	ProcessType   string
	GroupFilePath string
}

type ProcessInfo struct {
	Groups             *GroupInfo   // 组id信息
	CheckPointFilePath string       // 检查点文件路径
	Process            *ProcessFile // 组ID对应的进程文件信息
}

func ReadLine(fileName string) (res []string, err error) {
	file, err := os.OpenFile(fileName, os.O_RDONLY, 0660)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = file.Close()
	}()

	reader := bufio.NewReader(file)
	for {
		// 以换行符为界，分批次读取数据，得到readString
		line, _, err := reader.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return nil, err
			}
		}
		if len(string(line)) > 0 {
			res = append(res, string(line))
		}
	}
	return res, nil
}

func GetProcessAttribute(ct []string) (*ProcessFile, error) {
	p := new(ProcessFile)
	for _, s := range ct {
		ind := strings.Index(s, ":")
		if ind != -1 {
			n := s[:ind]
			v := s[ind+1:]
			switch {
			case strings.EqualFold(PROGRAM, strings.TrimSpace(n)):
				p.PROGRAM = strings.TrimSpace(v)
			case strings.EqualFold(PROCESSID, strings.TrimSpace(n)):
				p.PROCESSID = strings.TrimSpace(v)
			case strings.EqualFold(PORT, strings.TrimSpace(n)):
				p.PORT = strings.TrimSpace(v)
			case strings.EqualFold(PID, strings.TrimSpace(n)):
				p.PID = strings.TrimSpace(v)
			case strings.EqualFold(STATUS, strings.TrimSpace(n)):
				p.STATUS = strings.TrimSpace(v)
			case strings.EqualFold(DBTYPE, strings.TrimSpace(n)):
				p.DBTYPE = strings.TrimSpace(v)
			}
		}
	}
	return p, nil
}

// Will return true if the process with PID exists.
func CheckPid(pid int) bool {
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}

	err = process.Signal(syscall.Signal(0))
	if err != nil {
		return false
	} else {
		return true
	}
}

func ToBackground() {
	cmd := exec.Command(os.Args[0], flag.Args()...)
	if err := cmd.Start(); err != nil {
		os.Exit(137)
	}
	fmt.Printf("Process group is starting\n")
	os.Exit(0)
}

func CheckPcsFile(p string) (bool, error) {
	dir, err := GetHomeDirectory()
	if err != nil {
		return true, err
	}
	return IsFileExist(filepath.Join(*dir, "pcs", strings.ToUpper(p))), nil
}

func ParseNLSLANG(p string) (language, territory, charset string, err error) {
	if len(p) == 0 {
		return "", "", "", errors.Errorf("parse NLS_ LANG failed. the cannot be empty")
	}
	res := strings.Split(strings.ToUpper(p), ".")
	if len(res) == 2 {
		r := strings.Split(res[0], "_")
		if len(r) == 2 {
			language = r[0]
			territory = r[1]
			charset = res[1]
		} else {
			return "", "", "", errors.Errorf("NLS_LANG Incorrect format: %v", p)
		}
	} else {
		return "", "", "", errors.Errorf("NLS_LANG Incorrect format: %v", p)
	}

	return language, territory, charset, nil
}
