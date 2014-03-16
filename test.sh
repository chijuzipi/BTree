rm -rf mydisk*
makedisk mydisk 1024 128 1 16 64 100 10 .28
btree_init mydisk 64 16 15

btree_insert mydisk 64 eeeeeeeeee vvvvvvvvvvv
btree_insert mydisk 64 aaaaaaaaaaaaaaaa vvvvvvvvvvv
btree_insert mydisk 64 hhhhhhhhhh vvvvvvvvvvv
btree_insert mydisk 64 bbbbbbbbbb vvvvvvvvvvv
btree_insert mydisk 64 qqqqqqqqqq vvvvvvvvvvv
btree_insert mydisk 64 cccccccccc vvvvvvvvvvv
btree_insert mydisk 64 dddddddddd vvvvvvvvvvv
btree_insert mydisk 64 iiiiiiiiii vvvvvvvvvvv
btree_insert mydisk 64 ffffffffff vvvvvvvvvvv
btree_insert mydisk 64 uuuuuuuuuu vvvvvvvvvvv
btree_insert mydisk 64 gggggggggg vvvvvvvvvvv
btree_insert mydisk 64 jjjjjjjjjj vvvvvvvvvvv
btree_insert mydisk 64 zzzzzzzzzz vvvvvvvvvvv
btree_insert mydisk 64 wwwwwwwwww vvvvvvvvvvv
btree_insert mydisk 64 llllllllllllllll vvvvvvvvvvv
btree_insert mydisk 64 tttttttttt vvvvvvvvvvv
btree_insert mydisk 64 mmmmmmmmmm vvvvvvvvvvv
btree_insert mydisk 64 xxxxxxxxxx vvvvvvvvvvv
btree_insert mydisk 64 ssssssssss vvvvvvvvvvv
btree_insert mydisk 64 rrrrrrrrrr vvvvvvvvvvv
btree_insert mydisk 64 oooooooooo vvvvvvvvvvv
btree_insert mydisk 64 pppppppppp vvvvvvvvvvv
btree_insert mydisk 64 kkkkkkkkkk vvvvvvvvvvv
btree_insert mydisk 64 !!!!!!!!!!!!!!!! vvvvvvvvvvv

btree_display mydisk 64 normal
