package limit

import (
	"errors"
	"strconv"
	"time"

	"github.com/tal-tech/go-zero/core/stores/redis"
)

const (
	// to be compatible with aliyun redis, we cannot use `local key = KEYS[1]` to reuse the key
	// KEYS[1] 访问资源的标示
	// ARGV[1] limit => 请求总数，超过则限速。可设置为 QPS
	// ARGV[2] window => 滑动窗口大小，用 ttl 模拟出滑动的效果
	periodScript = `local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
-- incrbt key 1 => key visis++
local current = redis.call("INCRBY", KEYS[1], 1)
-- 如果是第一次访问，设置过期时间 => TTL = window size
-- 因为是只限制一段时间的访问次数
if current == 1 then
    redis.call("expire", KEYS[1], window)
    return 1
elseif current < limit then
    return 1
elseif current == limit then
    return 2
else
    return 0
end`
	zoneDiff = 3600 * 8 // GMT+8 for our services
)

const (
	// Unknown 返回值 未知
	Unknown = iota
	// Allowed 返回值 未达到配额
	Allowed
	// HitQuota 返回值 达到配额
	HitQuota
	// OverQuota 返回值 超过配额
	OverQuota

	// internalOverQuota 内部值 超过配额，对应 OverQuota
	internalOverQuota = 0
	// internalAllowed 内部值 未达到配额，对应 Allowed
	internalAllowed = 1
	// internalHitQuota 内部值 达到配额，对应 HitQuota
	internalHitQuota = 2
)

// ErrUnknownCode 未知错误类型
var ErrUnknownCode = errors.New("unknown status code")

type (
	// LimitOption 添加附加选项的方法，目前只提供了一个校准时区的功能
	LimitOption func(l *PeriodLimit)

	// PeriodLimit 窗口限流器
	PeriodLimit struct {
		// period 窗口时间窗口大小，单位为秒
		period int
		// quota 窗口内允许的请求数
		quota int
		// limitStore 窗口控制信息存储的位置，是不是有更美观的方式来设置存储位置？
		limitStore *redis.Redis
		// keyPrefix 窗口控制信息存储的key的前缀
		keyPrefix string
		// align 固定窗口开关，默认为滑动窗口
		// 默认为关闭状态，也就是普通的每 period 一个时间窗口。
		// 如果此开关为开，将在时间轴上以period的整数倍切割时间窗口，
		// 然后取当前时间戳到最近的分割点作为实际的窗口大小。
		// 比如period=5，则将时间轴切割为
		// .........................................
		// 0    5    10   15   20   25   * ^  *    *
		//                                 |
		//                             当前时间戳
		// 如果当前时间戳为 1613814217,正好在箭头所指的位置，距离当前的
		// 窗口结束还有3秒，所以实际的时间窗口大小就变成了3
		// 如果原本设计的是5秒处理150个请求，现在就变成了3秒处理150个请求，
		// 而且如果时间戳是 1613814219 这种的话就变成1秒处理150个请求了。
		// 所以这种方式不是很靠谱。
		// 可参考 https://www.infoq.cn/article/Qg2tX8fyw5Vt-f3HH673
		// 中的1、固定窗口计数器算法的解释
		align bool
	}
)

// NewPeriodLimit 创建滑动窗口实例
// period int 滑动窗口时间窗口大小，单位为秒
// quota int  滑动窗口内允许的请求数
// limitStore *redis.Redis 滑动窗口控制信息存储的位置
// keyPrefix string 滑动窗口控制信息存储的key的前缀
// opts ...LimitOption
func NewPeriodLimit(period, quota int, limitStore *redis.Redis, keyPrefix string,
	opts ...LimitOption) *PeriodLimit {
	limiter := &PeriodLimit{
		period:     period,
		quota:      quota,
		limitStore: limitStore,
		keyPrefix:  keyPrefix,
	}

	for _, opt := range opts {
		opt(limiter)
	}

	return limiter
}

// Take 获取访问状态
// 当访问成功时返回值为下列任意一个: Allowed|HitQuota|OverQuota
// 当访问失败时返回 Unknown 和错误信息
func (h *PeriodLimit) Take(key string) (int, error) {
	resp, err := h.limitStore.Eval(periodScript, []string{h.keyPrefix + key}, []string{
		strconv.Itoa(h.quota),
		strconv.Itoa(h.calcExpireSeconds()),
	})
	if err != nil {
		return Unknown, err
	}

	code, ok := resp.(int64)
	if !ok {
		return Unknown, ErrUnknownCode
	}

	switch code {
	case internalOverQuota:
		return OverQuota, nil
	case internalAllowed:
		return Allowed, nil
	case internalHitQuota:
		return HitQuota, nil
	default:
		return Unknown, ErrUnknownCode
	}
}

// calcExpireSeconds 计算时间窗口
// 以下支持两种时间窗口，固定窗口和非固定窗口。具体区别见 PeriodLimit 定义中的注释
func (h *PeriodLimit) calcExpireSeconds() int {
	if h.align {
		unix := time.Now().Unix() + zoneDiff
		return h.period - int(unix%int64(h.period))
	} else {
		return h.period
	}
}

// Align 窗口对齐
func Align() LimitOption {
	return func(l *PeriodLimit) {
		l.align = true
	}
}
