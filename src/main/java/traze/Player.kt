package traze

data class Player(
    var owned: Int = -1,
    var color: String = "",
    var name: String = "",
    var id: Int = -1,
    var frags: Int = -1
)