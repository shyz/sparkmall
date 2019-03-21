package com.atguigu.sparkmall

package object offline {
    def isNotEmpty(s: String) = s != null && s.length > 0
    def isEmpty(s: String) = !isNotEmpty(s)
}
