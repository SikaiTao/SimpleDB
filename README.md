# SimpleDB
Simple database implementation for assignment of NKU and its basic architecture goes from course CS186 of UCB
## 问题报告
### Lab 1
* __问题1:__ 在实现HeapPage.java中的isSlotUsed方法时，需要在header中判断某一位为0或1。问题在于，这里的成员变量为标头数组，即字节数组，无法高效地在数组中任意提取其中一个标头中的一位。  
* __解决方案:__ 利用计算机网络中掩码的思想，创建一个八位掩码，其中某一位（即想提取的对应位置）置1，其余位值0。将这个掩码与对应的header字节进行按位与的操作，按位与结果为0则该位为0，结果不为0则该位为1。  

* __问题2:__ 在实现HeapPage.java中的getNumEmptySlots方法时，需要在字节数组中按位计算0的个数，即循环取得每一字节计算其包含0的个数，之后累加即可。问题为如何找到一种高效的统计一个字节中0个数的算法。  
* __解决方案:__ 创建一个辅助函数countZero，输入参数为字节b，利用b&=b-1的核心算法进行实现，即在while循环中，每次进行b&b-1的操作并赋值，当b=0时停止，循环次数就是字节b中包含1的位的个数。用8减去这个数即为包含0的位的个数。这种算法性能和效率非常高。  

### Lab 2
* __问题1:__ 在实现聚合操作相关类时，即实现对应的数据库查询语句的“group by”关键字，这里所需要实现的最核心的类和函数为IntegerAggregator中的mergeTupleIntoGroup方法。但是遇到将会遇到一个问题：这个方法的输入值为一条元组t，无输出值，函数作用为更新聚合状态。即group by语句的底层实际上是动态地获取每一条元组，每获取一次更新聚合状态，而这个更新操作为“完备操作”——对于任意时刻的更新，当前的聚合状态均为这个时刻所获取的所有元组的最终聚合。而这里我们不能简单地采用“惰性操作”，即每获取一条元组t，都简单地保存这个元组，然后当聚合操作被调用获取结果集时，再相应地进行“average，max，min”等等操作，这种方法并不是最优的，因为首先我们需要时时刻刻保存所获取的所有元组，其次当多次调用获取结果集，将会进行多次重复计算操作。  
* __解决方案:__ mergeTupleIntoGroup获取每一个元组之后，都参与进行相关聚合操作的计算，更新内部状态之后即可丢弃该元组。需要实现的聚合操作有：sum、count、average、max和min五种，我们对应着建立四个哈希表：minMaxGroup、sumGroup、countGroup和averageGroup。它们的key为分组的字段gbfield，value为被聚合字段afield的状态值，例如平均值，最大/最小值等等。当一个新的元组t获取之后，先根据当前的操作装入不同的哈希表中，再判断这个哈希表是否预先含有这个元组的分组字段key，如是，则计算并更新value值（sum为叠加，min/max为比较更新，count为加一等等），反之则对于这个新的分组字段新建键值对。当调用获取结果集时，再将对应的哈希表转化为元组即可。  

* __问题2:__ 最后一个exercise需要实现Page Eviction策略，来使得当Page的缓存池存满之后，当新的一个Page读入时，将从缓存中寻找“最不常使用”的页，剔除它并置换成新的页。如何实现这个功能。  
* __解决方案:__ 类似于内存置换算法，我们将使用最简单但十分有效的LRU算法，在SimpleDB的BufferPool中实现，可以有三种代码编写方法：
  * 1)将页Page包装为一个新的对象，其中添加一个成员变量表示使用次数，每次被调用则次数加一，当置换时计算比较使用次数并选择次数最小的页。这种实现方法效率较低。
  * 2)新建一个链表作为BufferPool的Page容器来储存，当某一个Page被使用时，修改链表将它放在链表的尾部，则在每一时刻，此链表的头部是“最不常用”的页，进行Page置换时，将头部删去，并将新的Page置于尾部。
  * 3)Java.util.LinkedHashMap类已经实现了LRU算法，当初始化时令构造方法中的参数accessOrder为true时，则启用了LRU算法，当添加Page超过了其容量后，则自动置换最不常用的Page。  
__这里具体的代码实现使用了第三种方法。__  

### Lab 3  
在Lab3中，难点主要为实现B+树增删功能的核心函数，其中插入功能需要实现的核心函数为 __分裂__ ，删除功能需要实现的核心函数为 __平衡分配__ 和 __融合__ 。操作页面分为两种： __内部页__ 和 __叶子页__ 。以下根据增删功能的核心函数与操作页面相对应可得到Lab3需要实现的6个功能，并且根据代码实现逻辑列出步骤：  
1. __叶子页分裂__
  * 创造空页面作为分裂产生的新页面
  * 找到被分裂页面中间位置的元组作为分裂点
  * 迭代从被分裂页面到新页面顺次转移元组，当转移停止时，新页面的第一个元组为分裂点
  * 将分裂前的旧页面的右兄弟变为新页面的右兄弟
  * 将分裂点中的key提取并包装成为新的entry（分别指向被分裂页面和新页面），将这个新的entry插入到父节点中（内部页）
  * 更新对应的父指针  
2. __内部页分裂__
  * 创造空页面作为分裂产生的新页面
  * 找到被分裂页面中间位置的entry作为分裂点
  * 迭代从被分裂页面到新页面顺次转移entry，当迭代碰到分裂点转移停止，即新页面不包含分裂点，新页面的第一个entry为分裂点后面临近的第一个entry
  * 将分裂点中的key提取并包装成为新的entry分别指向被分裂页面和新页面），将这个新的entry插入到父节点中
  * 更新对应的父指针
3. __叶子页平衡分配__  
  * 平衡分配实际上就是元组较少的页面从元组较多的页面进行元组的“偷取”，从而达到平衡效果  
  * 根据两个页面存有的元组数量计算它们达到平衡所需要转移的元组个数：[T(多)+T(少)]/2-T(少)
  * 迭代从元组较多的页面顺次向元组较少的页面转移元组，当达到平衡个数停止
  * 将平衡分配后的右页面中的第一个元组中的key复制到对应的父节点中的entry，进行更新。
4. __内部页平衡分配__  
  * 基本步骤与3基本相同，只将其中的元组概念变为entry，但是其中最重要的不同点在于，在循环迭代转移entry的第一层，需要将两个页面指向的父entry作为第一个被转移的对象。以从左转移到右为例，在循环第一次时需要将父entry删除，并添加到右页面中。而到循环的最后一次时，需要将从左页面拿到的entry直接转移到父节点作为新的父entry。除此之外，在上述操作中，还需要更新对应的指针
5. __叶子页融合__
  * 将右页面的元组顺次转移到左页面
  * 将右页面的右兄弟更新为融合后的左页面的右兄弟
  * 将右页面设置为空页面，便于以后重用
  * 删除父entry，但保留父entry左边的邻居entry的右指针，此右指针指向融合后的左页面 
6. __内部页融合__
  * 复制父entry的key值，包装成为新entry，设置新entry的左指针指向左页面的最后一个孩子，右指针指向右页面的第一个孩子，将新entry添加到左页面
  * 之后将右页面的entry顺次转移到左页面
  * 更新左页面中转移后的所有entry的指针
  * 将右页面设置为空页面，便于以后重用
  * 删除父entry，但保留父entry左边的邻居entry的右指针，此右指针指向融合后的左页面

### Lab 4
本实验中，要求给事务加锁，并且实现NO STEAL和FORCE策略，再完成对死锁的检测。这里我采用__页面级加锁__ ，实验doc说明中也推荐了这种锁粒度类别，因为SimpleDB的设计模式导致查询与增删都会共同调用BufferPool中的getPage方法，在这一函数中由相应的事务id进行对操作页面pageId的加锁更加简洁有效，也满足较低耦合性的设计原则。下面介绍lab中遇到的几个难点和解决方案：
* 创建新类LocksCatalog来记录加锁信息，这个类维护一个映射关系，即每一个页面pageId都映射一个列表，这个列表中储存了若干个关于此页面的锁持有情况（事务id和锁类别）。这里我直接采用Permissions.READ_ONLY和Permissions.READ_WRITE作为参数传入来代表读锁和写锁。当一个事务对一个页面加锁时会调用LocksCatalog来托管完成，这里它会判断以下几个情况并做出返回true的处理：
  * 此页面无任何事务持有锁，此事务获得锁，无论锁的类别
  * 此事务在此页面已持有锁，且申请锁类型与已持有类型相同
  * 此事务在此页面已持有读锁，无其他事务持有锁，此事务申请写锁（升级）
  * 此事务在此页面已持有写锁，又申请读锁
  * 此事务不持有锁，来申请读锁，并且其他事务也对此页面持有读锁
  以上几个情况为锁申请成功，其他情况则申请失败，返回false
* 需要修改Lab1中已完成的HeapFile中的insertTuple函数，因为它对于每一个page检索判断有无空位插入时，申请的都是写锁，之前对这里的处理是若无空位则continue检查下一个page。__在Lab4中这里需要特殊地加上释放锁的语句。__
* 在之前的Lab中，BufferPool的实际数据结构采用LinkedHashMap，但它线程不安全，这里改为ConcurrentHashMap
* 采用FORCE策略，在transactionComplete函数中判断是否commit，如果是，则利用flushPages函数将该事务所修改的脏页面全部持久化到磁盘中，如果不是则将该事务所修改的脏页面通过Catalog回退到原始页面
* 采用NO STEAL策略，需要修改前面lab实现的evictPage函数：在缓存中找到非脏页面并进行剔除，如果全为脏页面则抛出异常
* 关于线程对于锁的竞争和死锁判断：在每一个事务线程调用getPage函数时，都会通过LocksCatalog来委托获取锁，若获取失败，则随机休息0\~50ms，之后再重新尝试加锁。在第一次尝试申请时记录时间戳，当超过时间限度时，则抛出异常代表死锁发生，这个时间限度我设置为固定2000ms加随机0\~500ms。以上所规定的随机时间均为了让各个线程错开申请锁的时间，来避开锁的竞争频率。  

### Lab 5
* 对数据表中关于某一个field进行filter查询，需要借助直方图来预估它的selectivity（选择率），实际上选择率指的是filter得到的元组数占总元组数的比例。在计算选择率时，可以用一个代码技巧：复用函数。即面对许六种filter的谓词(<,≤,≥,>,=,≠)，只需要编写谓词是“小于”的选择率预估函数即可，其他五种谓词都可以由其推导出来，假设已经实现了函数selectivity(<,value)：
  * selectivity(≤,value) = selectivity(<,value+1)
  * selectivity(>,value) = 1 - selectivity(≤,value)
  * selectivity(≥,value) = 1 - selectivity(<,value)
  * selectivity(=,value) = selectivity(≤,value) - selectivity(<,value)
  * selectivity(≠,value) = 1 - selectivity(=,value)
* 类TableStats实际上就是对数据表schema中的每一个field都建立一个直方图。以及利用直方图类方法，根据filter所针对的field，计算预估的选择率。
* 对于表连接（TableJoin），需要实现对于Cardinality（连接数）预估，连接基数指的是表连接后的对于元组个数的估计，选择数预估的计算分为三种情况，假设t1和t2两个表进行连接（但连接存在嵌套情况，即t1和t2位置可能是两个连接结果的子表）：
  * 如果连接谓词是equal，且连接字段在表t1中是主键，则此连接的选择数为t2的选择数；相应地，连接字段在表t2中是主键，则此连接的选择数为t1的选择数；
  * 如果连接谓词是equal，但该连接字段在t1和t2中均不作为主键，则t1，t2两表之间相比较大的选择数作为该连接的选择数；
  * 如果连接谓词不是equal，情况较为复杂，但在Simple要求下，可以设置一个比例（例如30%），此连接的选择数计算等于t1和t2的选择数相乘，再乘以预设的比例。  
其实对于整个Lab5来说，只需要关注并计算Join的CPU运算开销和表访问的基础开销（I/O代价），而不需要关心每一个selection的具体访问方式，因为在整个SimpleDB中，表的访问方式只有scan一种。而计算基础开销，其中最重要的元素就是选择数，选择数的计算在底层单个表上，需要落实到以直方图为基础的预估方法。而在上层对于多个表或子表进行连接查询时，则需要依靠定义的Join选择数的预估法则来进行计算。



