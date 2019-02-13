package shardmaster

import (
	"github.com/luci/go-render/render"
	"testing"
)

func Test_rebalanceConfig(t *testing.T) {
	sm := ShardMaster{
		configs: []Config{},
	}
	confs := []*Config{
		// join operation
		{
			Num:    1,
			Shards: [10]int{},
			Groups: map[int][]string{
				1: {"1", "2", "3"},
			},
		},
		{
			Num:    1,
			Shards: [10]int{},
			Groups: map[int][]string{
				1: {"1", "2", "3"},
				2: {"4", "5", "6"},
				3: {"4", "5", "6"},
			},
		},
		{
			Num:    1,
			Shards: [10]int{1,1,1,2,2,2,3,3,3,3},
			Groups: map[int][]string{
				1: {"1", "2", "3"},
				2: {"4", "5", "6"},
				3: {"4", "5", "6"},
				4: {"4", "5", "6"},
			},
		},
		{
			Num:    1,
			Shards: [10]int{1,2,3,4,5,6,7,8,9,9},
			Groups: map[int][]string{
				1: {"1", "2", "3"},
				2: {"4", "5", "6"},
				3: {"4", "5", "6"},
				4: {"4", "5", "6"},
				5: {"4", "5", "6"},
				6: {"4", "5", "6"},
				7: {"4", "5", "6"},
				8: {"4", "5", "6"},
				9: {"4", "5", "6"},
				10: {"4", "5", "6"},
			},
		},
		{
			Num:    1,
			Shards: [10]int{1,2,3,4,5,6,6,1,2,3},
			Groups: map[int][]string{
				1: {"1", "2", "3"},
				2: {"4", "5", "6"},
				3: {"4", "5", "6"},
				4: {"4", "5", "6"},
				5: {"4", "5", "6"},
				6: {"4", "5", "6"},
				7: {"4", "5", "6"},
			},
		},

		// move operation
		//{
		//	Num:    1,
		//	Shards: [10]int{1,1,1,1,3,3,4,5,6,7},
		//	Groups: map[int][]string{
		//		1: {"1", "2", "3"},
		//		2: {"4", "5", "6"},
		//		3: {"4", "5", "6"},
		//		4: {"4", "5", "6"},
		//		5: {"4", "5", "6"},
		//		6: {"4", "5", "6"},
		//		7: {"4", "5", "6"},
		//	},
		//},
	}

	for _, v := range confs {
		sm.rebalanceConfig(v)
		t.Logf("%v", render.Render(v))
	}

}

func Test_sortMap(t *testing.T) {
	m := map[int][]int{
		1: {8},
		2: {1, 6, 3, 4},
		4: {2},
	}
	pairs := sortMap(m, false)
	t.Logf("%v", pairs)

	pairs = sortMap(m, true)
	t.Logf("%v", pairs)
}
