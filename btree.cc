#include <assert.h>
#include <math.h>
#include <string.h>
#include "btree.h"

KeyValuePair::KeyValuePair()
{}


KeyValuePair::KeyValuePair(const KEY_T &k, const VALUE_T &v) : 
  key(k), value(v)
{}


KeyValuePair::KeyValuePair(const KeyValuePair &rhs) :
  key(rhs.key), value(rhs.value)
{}


KeyValuePair::~KeyValuePair()
{}


KeyValuePair & KeyValuePair::operator=(const KeyValuePair &rhs)
{
  return *( new (this) KeyValuePair(rhs));
}

BTreeIndex::BTreeIndex(SIZE_T keysize, 
           SIZE_T valuesize,
           BufferCache *cache,
           bool unique) 
{
  superblock.info.keysize=keysize;
  superblock.info.valuesize=valuesize;
  buffercache=cache;
  // note: ignoring unique now
}

BTreeIndex::BTreeIndex()
{
  // shouldn't have to do anything
}


//
// Note, will not attach!
//
BTreeIndex::BTreeIndex(const BTreeIndex &rhs)
{
  buffercache=rhs.buffercache;
  superblock_index=rhs.superblock_index;
  superblock=rhs.superblock;
}

BTreeIndex::~BTreeIndex()
{
  // shouldn't have to do anything
}


BTreeIndex & BTreeIndex::operator=(const BTreeIndex &rhs)
{
  return *(new(this)BTreeIndex(rhs));
}


ERROR_T BTreeIndex::AllocateNode(SIZE_T &n)
{
  n=superblock.info.freelist;

  if (n==0) { 
    return ERROR_NOSPACE;
  }

  BTreeNode node;

  node.Unserialize(buffercache,n);

  assert(node.info.nodetype==BTREE_UNALLOCATED_BLOCK);

  superblock.info.freelist=node.info.freelist;

  superblock.Serialize(buffercache,superblock_index);

  buffercache->NotifyAllocateBlock(n);

  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::DeallocateNode(const SIZE_T &n)
{
  BTreeNode node;

  node.Unserialize(buffercache,n);

  assert(node.info.nodetype!=BTREE_UNALLOCATED_BLOCK);

  node.info.nodetype=BTREE_UNALLOCATED_BLOCK;

  node.info.freelist=superblock.info.freelist;

  node.Serialize(buffercache,n);

  superblock.info.freelist=n;

  superblock.Serialize(buffercache,superblock_index);

  buffercache->NotifyDeallocateBlock(n);

  return ERROR_NOERROR;

}

ERROR_T BTreeIndex::Attach(const SIZE_T initblock, const bool create)
{
  ERROR_T rc;

  superblock_index=initblock;
  assert(superblock_index==0);

  if (create) {
    // build a super block, root node, and a free space list
    //
    // Superblock at superblock_index
    // root node at superblock_index+1
    // free space list for rest
    BTreeNode newsuperblock(BTREE_SUPERBLOCK,
          superblock.info.keysize,
          superblock.info.valuesize,
          buffercache->GetBlockSize());
    newsuperblock.info.rootnode=superblock_index+1;
    newsuperblock.info.freelist=superblock_index+2;
    newsuperblock.info.numkeys=0;

    buffercache->NotifyAllocateBlock(superblock_index);

    rc=newsuperblock.Serialize(buffercache,superblock_index);

    if (rc) { 
      return rc;
    }
    
    BTreeNode newrootnode(BTREE_ROOT_NODE,
        superblock.info.keysize,
        superblock.info.valuesize,
        buffercache->GetBlockSize());
    newrootnode.info.rootnode=superblock_index+1;
    newrootnode.info.freelist=superblock_index+2;
    newrootnode.info.numkeys=0;

    buffercache->NotifyAllocateBlock(superblock_index+1);

    rc=newrootnode.Serialize(buffercache,superblock_index+1);

    if (rc) { 
      return rc;
    }

    for (SIZE_T i=superblock_index+2; i<buffercache->GetNumBlocks();i++) { 
      BTreeNode newfreenode(BTREE_UNALLOCATED_BLOCK,
          superblock.info.keysize,
          superblock.info.valuesize,
          buffercache->GetBlockSize());
      newfreenode.info.rootnode=superblock_index+1;
      newfreenode.info.freelist= ((i+1)==buffercache->GetNumBlocks()) ? 0: i+1;
      
      rc = newfreenode.Serialize(buffercache,i);

      if (rc) {
        return rc;
      }

    }
  }

  // OK, now, mounting the btree is simply a matter of reading the superblock 

  return superblock.Unserialize(buffercache,initblock);
}
    

ERROR_T BTreeIndex::Detach(SIZE_T &initblock)
{
  return superblock.Serialize(buffercache,superblock_index);
}
 

ERROR_T BTreeIndex::LookupOrUpdateInternal(const SIZE_T &node,
             const BTreeOp op,
             const KEY_T &key,
             VALUE_T &value)
{
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;
  KEY_T testkey;
  SIZE_T ptr;

  rc= b.Unserialize(buffercache,node);

  if (rc!=ERROR_NOERROR) { 
    return rc;
  }

  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    // Scan through key/ptr pairs
    //and recurse if possible
    for (offset=0;offset<b.info.numkeys;offset++) { 
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (key<testkey) {
  // OK, so we now have the first key that's larger
  // so we ned to recurse on the ptr immediately previous to 
  // this one, if it exists
  rc=b.GetPtr(offset,ptr);
  if (rc) { return rc; }
  return LookupOrUpdateInternal(ptr,op,key,value);
      }
    }
    // if we got here, we need to go to the next pointer, if it exists
    if (b.info.numkeys>0) { 
      rc=b.GetPtr(b.info.numkeys,ptr);
      if (rc) { return rc; }
      return LookupOrUpdateInternal(ptr,op,key,value);
    } else {
      // There are no keys at all on this node, so nowhere to go
      return ERROR_NONEXISTENT;
    }
    break;
  case BTREE_LEAF_NODE:
    // Scan through keys looking for matching value
    for (offset=0;offset<b.info.numkeys;offset++) { 
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (testkey==key) { 
  if (op==BTREE_OP_LOOKUP) { 
    return b.GetVal(offset,value);
  } else { 
    rc = b.SetVal(offset,value);
    if (rc) { return rc; }

    rc = b.Serialize(buffercache, node);
    if (rc) { return rc; }

    return ERROR_NOERROR;
  }
      }
    }
    return ERROR_NONEXISTENT;
    break;
  default:
    // We can't be looking at anything other than a root, internal, or leaf
    return ERROR_INSANE;
    break;
  }  

  return ERROR_INSANE;
}


static ERROR_T PrintNode(ostream &os, SIZE_T nodenum, BTreeNode &b, BTreeDisplayType dt)
{
  KEY_T key;
  VALUE_T value;
  SIZE_T ptr;
  SIZE_T offset;
  ERROR_T rc;
  unsigned i;

  if (dt==BTREE_DEPTH_DOT) { 
    os << nodenum << " [ label=\""<<nodenum<<": ";
  } else if (dt==BTREE_DEPTH) {
    os << nodenum << ": ";
  } else {
  }

  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    if (dt==BTREE_SORTED_KEYVAL) {
    } else {
      if (dt==BTREE_DEPTH_DOT) { 
      } else { 
  os << "Interior: ";
      }
      for (offset=0;offset<=b.info.numkeys;offset++) { 
  rc=b.GetPtr(offset,ptr);
  if (rc) { return rc; }
  os << "*" << ptr << " ";
  // Last pointer
  if (offset==b.info.numkeys) break;
  rc=b.GetKey(offset,key);
  if (rc) {  return rc; }
  for (i=0;i<b.info.keysize;i++) { 
    os << key.data[i];
  }
  os << " ";
      }
    }
    break;
  case BTREE_LEAF_NODE:
    if (dt==BTREE_DEPTH_DOT || dt==BTREE_SORTED_KEYVAL) { 
    } else {
      os << "Leaf: ";
    }
    for (offset=0;offset<b.info.numkeys;offset++) { 
      if (offset==0) { 
  // special case for first pointer
  rc=b.GetPtr(offset,ptr);
  if (rc) { return rc; }
  if (dt!=BTREE_SORTED_KEYVAL) { 
    os << "*" << ptr << " ";
  }
      }
      if (dt==BTREE_SORTED_KEYVAL) { 
  os << "(";
      }


      rc=b.GetKey(offset,key);
      if (rc) {  return rc; }
      for (i=0;i<b.info.keysize;i++) { 
  os << key.data[i];
      }
      if (dt==BTREE_SORTED_KEYVAL) { 
  os << ",";
      } else {
  os << " ";
      }
      rc=b.GetVal(offset,value);
      if (rc) {  return rc; }
      for (i=0;i<b.info.valuesize;i++) { 
  os << value.data[i];
      }
      if (dt==BTREE_SORTED_KEYVAL) { 
  os << ")\n";
      } else {
  os << " ";
      }
    }
    break;
  default:
    if (dt==BTREE_DEPTH_DOT) { 
      os << "Unknown("<<b.info.nodetype<<")";
    } else {
      os << "Unsupported Node Type " << b.info.nodetype ;
    }
  }
  if (dt==BTREE_DEPTH_DOT) { 
    os << "\" ]";
  }
  return ERROR_NOERROR;
}
  
ERROR_T BTreeIndex::Lookup(const KEY_T &key, VALUE_T &value)
{
  return LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_LOOKUP, key, value);
}


ERROR_T BTreeIndex::Insert(const KEY_T &key, const VALUE_T &value)
{
  list<SIZE_T> clues;
  KEY_T instkey = key;//since during split and pop, the key been poped may change
  
  bool pop = true;
  BTreeNode b;
  ERROR_T rc;
  SIZE_T ptr;

  // First look up which leaf should insert to, and record clues
  rc = LookupInsertion(clues, superblock.info.rootnode, instkey);
  if(rc != ERROR_NOERROR) {return rc;}

  while (!clues.empty() && pop == true){
    rc = b.Unserialize(buffercache, clues.front());
    if (rc!=ERROR_NOERROR) { return rc; }
    
    rc = InsertNode(b, instkey, value, ptr, pop); //only call InsertNode once
    if (rc != ERROR_NOERROR) { return rc; }

    rc = b.Serialize(buffercache, clues.front());
    if (rc != ERROR_NOERROR) { return rc; }

    clues.pop_front(); // remove this node
  }
  return ERROR_NOERROR;
}

ERROR_T BTreeIndex::LookupInsertion(list<SIZE_T> &clues, const SIZE_T &blocknum, const KEY_T &key) 
{
  BTreeNode node;
  ERROR_T rc;
  KEY_T tempkey;
  SIZE_T offset;
  SIZE_T tempptr;

  clues.push_front(blocknum);

  rc = node.Unserialize(buffercache, blocknum);

  if (rc != ERROR_NOERROR) {
    return rc;
  }

  switch(node.info.nodetype) { 
    case BTREE_ROOT_NODE:
    case BTREE_INTERIOR_NODE: {
      for (offset = 0; offset < node.info.numkeys; offset++) { 
        rc = node.GetKey(offset, tempkey);
        if (rc) { return rc; }
  
        if (key < tempkey) {
          rc = node.GetPtr(offset, tempptr);
          if (rc) { return rc; }
  
          return LookupInsertion(clues, tempptr, key);
        }
      }
  
      //the scan goes to the last key in the node (return the point of the right)
      if (node.info.numkeys > 0) { 
        rc = node.GetPtr(node.info.numkeys, tempptr);
        if (rc) { return rc; }
  
        return LookupInsertion(clues, tempptr, key);
  
      } else {
        // the node is empty
        return ERROR_NOERROR;
      }
  
      break;
    }
  
    case BTREE_LEAF_NODE:
      // direct return the node pointer
      return ERROR_NOERROR;
  } 
  return ERROR_NOERROR;
}

ERROR_T BTreeIndex::InsertNode(BTreeNode &node, KEY_T &key, 
                               const VALUE_T &value, SIZE_T &ptr, bool &pop)
{  
  ERROR_T rc;

  SIZE_T MaxKeysNumber;
  switch (node.info.nodetype) {
    case BTREE_ROOT_NODE:
    case BTREE_INTERIOR_NODE:
      MaxKeysNumber = node.info.GetNumSlotsAsInterior();
      break;
    case BTREE_LEAF_NODE:
      MaxKeysNumber = node.info.GetNumSlotsAsLeaf();
  }
  
  if (node.info.numkeys == 0) {
    switch(node.info.nodetype) {
      case BTREE_ROOT_NODE: {
        // ----------------------------------
        // Initial condition: empty root node
        // ----------------------------------
        // Make one new leaf node to store the key and value
        BTreeNode newleaf(BTREE_LEAF_NODE,
                          superblock.info.keysize,
                          superblock.info.valuesize,
                          buffercache -> GetBlockSize());
        newleaf.info.rootnode = superblock_index + 1;
        newleaf.info.numkeys = 1;
  
        rc = newleaf.SetKey(0, key);
        if (rc) { return rc; }
        rc = newleaf.SetVal(0, value);
        if (rc) { return rc; }
  
        // Write the leaf to block
        SIZE_T newleafptr;
        rc = AllocateNode(newleafptr);
        if (rc) { return rc; }
      
        rc = newleaf.Serialize(buffercache, newleafptr);
        if (rc) { return rc; }

        // ----------------------------------
        // Set up the root node
        // ----------------------------------
        //
        // Set number of keys in root to 1
        node.info.numkeys = 1;
  
        // Set key in root
        rc = node.SetKey(0, key);
        if (rc) { return rc; }
  
        // Set both ptr of root to point at
        // the only leaf node
        rc = node.SetPtr(0, newleafptr);
        if (rc) { return rc; }
        rc = node.SetPtr(1, newleafptr);
        if (rc) { return rc; }
  
        rc = node.Serialize(buffercache, superblock_index + 1);
        if (rc) { return rc; }

        // No need to pop, insertion is done
        pop = false;

        break;
      
      }
      // No interior is allowed to be empty
      case BTREE_INTERIOR_NODE:
        return ERROR_INSANE;
    
      case BTREE_LEAF_NODE: {
        rc = node.SetKey(0, key);
        if (rc) { return rc; }
        rc = node.SetVal(0, value);
        if (rc) { return rc; }

        break;
      }
      default:
        return ERROR_INSANE;
    }
  } else if (node.info.numkeys > 0 && 
             node.info.numkeys < MaxKeysNumber) {

    rc = InsertNonFull(node, key, value, ptr);
    if (rc) { return rc; }

    pop = false;

    return ERROR_NOERROR;

  } else if (node.info.numkeys == MaxKeysNumber) {
    // When the node is full, need to split and pop
    rc = InsertFull(node, key, value, ptr, pop);
    if (rc) { return rc; }

    return ERROR_NOERROR;
  }  
  return ERROR_NOERROR;
}

ERROR_T BTreeIndex::InsertFull (BTreeNode &oldnode, KEY_T &key, 
                                const VALUE_T &value, SIZE_T &ptr, bool &pop)
{
  // Determine the insert position
  ERROR_T rc;
  SIZE_T inspos;
  SIZE_T offset;
  KEY_T tempkey;
  SIZE_T tempptr;
  VALUE_T tempval;

  for (offset = 0; offset < oldnode.info.numkeys; offset++) {
    rc = oldnode.GetKey(offset, tempkey);
    if (rc) { return rc; }

    if (key == tempkey) { 
      return ERROR_CONFLICT;
    } else if (key < tempkey) {
      break; 
    }
  }
  inspos = offset;

  // Determine the partition position
  //   origmed: median position before insertion
  //   median: median position after insertion
  SIZE_T nslots;
  switch (oldnode.info.nodetype) {
    case BTREE_ROOT_NODE:
    case BTREE_INTERIOR_NODE:
      nslots = oldnode.info.GetNumSlotsAsInterior();
      break;
    case BTREE_LEAF_NODE:
      nslots = oldnode.info.GetNumSlotsAsLeaf();
  }
  
  SIZE_T origmed = ceil(nslots / 2.0) - 1;
  SIZE_T median;
  SIZE_T i;

  if (inspos <= origmed) {
    median = origmed;
  } else {
    median = origmed + 1;
  }

  switch(oldnode.info.nodetype) {
    // ----------------------------------
    // BTREE_ROOT_NODE
    // ----------------------------------
    // Two nodes are needed when root need to be splitted.
    // The keys & ptrs are distributed to the left new node
    // and the right node. Then the root node is replaced
    // with just one popped key and two pointers referring to
    // the two new nodes.
    case BTREE_ROOT_NODE: {
      // Create two new nodes
      BTreeNode newleftnode(BTREE_INTERIOR_NODE,
                            superblock.info.keysize,
                            superblock.info.valuesize,
                            buffercache -> GetBlockSize());
      newleftnode.info.rootnode = superblock_index + 1;
      newleftnode.info.numkeys = 0;

      BTreeNode newrightnode(BTREE_INTERIOR_NODE,
                             superblock.info.keysize,
                             superblock.info.valuesize,
                             buffercache -> GetBlockSize());
      newrightnode.info.rootnode = superblock_index + 1;
      newrightnode.info.numkeys = 0;

      // Move the first half of root node into left new node
      for (i = 0; i < median; i++) {
        newleftnode.info.numkeys++;

        rc = oldnode.GetKey(i, tempkey);
        if (rc) { return rc; }
        rc = oldnode.GetPtr(i, tempptr);
        if (rc) { return rc; }

        rc = newleftnode.SetKey(i, tempkey);
        if (rc) { return rc; }
        rc = newleftnode.SetPtr(i, tempptr);
        if (rc) { return rc; }
      }
      rc = oldnode.GetPtr(median, tempptr);
      if (rc) { return rc; }
      rc = newleftnode.SetPtr(median, tempptr);
      if (rc) { return rc; }

      // Move the other half of root node into right new node
      for (i = median; i < nslots; i++) {
        newrightnode.info.numkeys++;

        rc = oldnode.GetKey(i, tempkey);
        if (rc) { return rc; }
        rc = oldnode.GetPtr(i, tempptr);
        if (rc) { return rc; }

        rc = newrightnode.SetKey(i - median, tempkey);
        if (rc) { return rc; }
        rc = newrightnode.SetPtr(i - median, tempptr);
        if (rc) { return rc; }
      }
      rc = oldnode.GetPtr(nslots, tempptr);
      if (rc) { return rc; }
      rc = newrightnode.SetPtr(nslots - median, tempptr);
      if (rc) { return rc; }

      // Insert the new key and ptr to either node
      if (inspos <= origmed) {
        rc = InsertNonFull(newleftnode, key, value, ptr);
        if (rc) { return rc; }
      } else {
        rc = InsertNonFull(newrightnode, key, value, ptr);
        if (rc) { return rc; }
      }

      // Two new blocks needed for the nodes
      SIZE_T leftptr;
      SIZE_T rightptr;

      rc = AllocateNode(leftptr);
      if (rc) { return rc; }
      rc = AllocateNode(rightptr);
      if (rc) { return rc; }

      // Get the median key to promote
      rc = newrightnode.GetKey(0, key);
      if (rc) { return rc; }

      // Promote the median key and delete from the right node
      for (i = 1; i < newrightnode.info.numkeys; i++) {
        rc = newrightnode.GetKey(i, tempkey);
        if (rc) { return rc; }
        rc = newrightnode.GetPtr(i, tempptr);
        if (rc) { return rc; }

        rc = newrightnode.SetKey(i - 1, tempkey);
        if (rc) { return rc; }
        rc = newrightnode.SetPtr(i - 1, tempptr);
        if (rc) { return rc; }
      }
      rc = newrightnode.GetPtr(newrightnode.info.numkeys, tempptr);
      if (rc) { return rc; }
      rc = newrightnode.SetPtr(newrightnode.info.numkeys - 1, tempptr);
      if (rc) { return rc; }

      newrightnode.info.numkeys--;

      // Write the changes to the block
      rc = newleftnode.Serialize(buffercache, leftptr);
      if (rc) { return rc; }
      rc = newrightnode.Serialize(buffercache, rightptr);
      if (rc) { return rc; }

      // Rewrite the root node with the promoted key
      rc = oldnode.SetKey(0, key);
      if (rc) { return rc; }
      rc = oldnode.SetPtr(0, leftptr);
      if (rc) { return rc; }
      rc = oldnode.SetPtr(1, rightptr);
      if (rc) { return rc; }
      oldnode.info.numkeys = 1;

      rc = oldnode.Serialize(buffercache, superblock_index + 1);
      if (rc) { return rc; }

      pop = false;

      break;
    }

    // ----------------------------------
    // BTREE_INTERIOR_NODE
    // ----------------------------------
    //
    case BTREE_INTERIOR_NODE: {
      BTreeNode newnode(BTREE_INTERIOR_NODE,
                        superblock.info.keysize,
                        superblock.info.valuesize,
                        buffercache -> GetBlockSize());
      newnode.info.rootnode = superblock_index + 1;
      newnode.info.numkeys = 0;
      
      for (i = median; i < nslots; i++) {
        newnode.info.numkeys++;

        rc = oldnode.GetKey(i, tempkey);
        if (rc) { return rc; }
        rc = oldnode.GetPtr(i, tempptr);
        if (rc) { return rc; }

        rc = newnode.SetKey(i - median, tempkey);
        if (rc) { return rc; }
        rc = newnode.SetPtr(i - median, tempptr);
        if (rc) { return rc; }

      }
      rc = oldnode.GetPtr(nslots, tempptr);
      if (rc) { return rc; }
      rc = newnode.SetPtr(nslots - median, tempptr);
      if (rc) { return rc; }

      oldnode.info.numkeys = oldnode.info.numkeys - nslots + median;

      // Insert the new key and ptr to either node
      if (inspos <= origmed) {
        rc = InsertNonFull(oldnode, key, value, ptr);
        if (rc) { return rc; }
      } else {
        rc = InsertNonFull(newnode, key, value, ptr);
        if (rc) { return rc; }
      }

      rc = AllocateNode(ptr);
      if (rc) { return rc; }

      rc = newnode.GetKey(0, key);
      if (rc) { return rc; }

      // Promote the median key and delete from the new node
      for (i = 1; i < newnode.info.numkeys; i++) {
        rc = newnode.GetKey(i, tempkey);
        if (rc) { return rc; }
        rc = newnode.GetPtr(i, tempptr);
        if (rc) { return rc; }

        rc = newnode.SetKey(i - 1, tempkey);
        if (rc) { return rc; }
        rc = newnode.SetPtr(i - 1, tempptr);
        if (rc) { return rc; }
      }
      rc = newnode.GetPtr(newnode.info.numkeys, tempptr);
      if (rc) { return rc; }
      rc = newnode.SetPtr(newnode.info.numkeys - 1, tempptr);
      if (rc) { return rc; }

      newnode.info.numkeys--;

      // Write the changes to the block
      rc = newnode.Serialize(buffercache, ptr);
      if (rc) { return rc; }

      pop = true;

      break;
    }
    
    // ----------------------------------
    // BTREE_LEAF_NODE
    // ----------------------------------
    //
    case BTREE_LEAF_NODE: {
      BTreeNode newnode(BTREE_LEAF_NODE,
                        superblock.info.keysize,
                        superblock.info.valuesize,
                        buffercache -> GetBlockSize());
      newnode.info.rootnode = superblock_index + 1;
      newnode.info.numkeys = 0;

      for (i = median; i < nslots; i++) {
        newnode.info.numkeys++;

        rc = oldnode.GetKey(i, tempkey);
        if (rc) {return rc;}
        rc = oldnode.GetVal(i, tempval);
        if (rc) {return rc;}

        rc = newnode.SetKey(i - median, tempkey);
        if (rc) {return rc;}
        rc = newnode.SetVal(i - median, tempval);
        if (rc) {return rc;}
      }
      oldnode.info.numkeys = oldnode.info.numkeys - nslots + median;

      if (inspos <= origmed) {
        rc = InsertNonFull(oldnode, key, value, ptr);
        if (rc) {return rc;}
      } else {
        rc = InsertNonFull(newnode, key, value, ptr);
        if (rc) {return rc;}
      }

      // Allocate block for the node
      // Update the ptr for next recursion
      rc = AllocateNode(ptr);
      if (rc) {return rc;}

      // Update the key for next recursion
      rc = newnode.GetKey(0, key);
      if (rc) {return rc;}

      // Fast range query (B+ Tree)
      // Link the old node to the new node
      rc = oldnode.GetPtr(0, tempptr);
      if (rc) {return rc;}

      rc = newnode.SetPtr(0, tempptr);
      if (rc) {return rc;}

      rc = oldnode.SetPtr(0, ptr);
      if (rc) {return rc;}

      rc = newnode.Serialize(buffercache, ptr);
      if (rc) {return rc;}

      pop = true;

      break;
    }

    default:
      return ERROR_INSANE;
  }
  return ERROR_NOERROR;
}

ERROR_T BTreeIndex::InsertNonFull (BTreeNode &node, KEY_T &key, 
                                   const VALUE_T &value, SIZE_T &ptr)
{
  ERROR_T rc;

  KEY_T tempkey;
  SIZE_T tempptr;
  VALUE_T tempval;
  KeyValuePair temppair;

  SIZE_T offset;

  for (offset = 0; offset < node.info.numkeys; offset++) {
    // Move through keys until we find one larger than input key
    rc = node.GetKey(offset, tempkey);
    if (rc) { return rc; }

    // Otherwise, break loop
    if (key < tempkey) { break; }
  }

  SIZE_T leftptr;
  SIZE_T rightptr;

  int i;
  node.info.numkeys++;
  for (i = node.info.numkeys - 2; i >= int(offset); i--) {
    switch(node.info.nodetype) {
      case BTREE_ROOT_NODE:
      case BTREE_INTERIOR_NODE:

        // Shift key
        rc = node.GetKey(i, tempkey);
        if (rc) { return rc; }

        rc = node.SetKey(i + 1, tempkey);
        if (rc) { return rc; }
        
        // Shift pointer
        rc = node.GetPtr(i + 1, tempptr);
        if (rc) { return rc; }

        rc = node.SetPtr(i + 2, tempptr);
        if (rc) { return rc; }

        break;

      case BTREE_LEAF_NODE:
        rc = node.GetKeyVal(i, temppair);
        if (rc) { return rc; }

        rc = node.SetKeyVal(i + 1, temppair);
        break;

      default:
        return ERROR_INSANE;
    }
  }

  // Set input key
  rc = node.SetKey(offset, key);
  if (rc) { return rc; }

  // Set input ptr
  if(node.info.nodetype == BTREE_LEAF_NODE) {
    rc = node.SetVal(offset, value);
    if (rc) { return rc; }
  }

  if(node.info.nodetype == BTREE_ROOT_NODE or 
     node.info.nodetype == BTREE_INTERIOR_NODE) {
    rc = node.SetPtr(offset + 1, ptr);
    if (rc) { return rc; }
  }

  // ----------------------------------
  // BTREE_ROOT_NODE (SPECIAL CASE)
  // ----------------------------------
  // When only 1 key exist in the root node 
  // and the left/right pointers are the same,
  // rewrite the key and the right pointer.
  // (Special initial case)
  if (node.info.nodetype == BTREE_ROOT_NODE) {
    rc = node.GetPtr(0, leftptr);
    if (rc) { return rc; }
    rc = node.GetPtr(1, rightptr);
    if (rc) { return rc; }

    if (leftptr == rightptr) {
      rc = node.SetKey(0, key);
      if (rc) { return rc; }
      rc = node.SetPtr(1, ptr);
      if (rc) { return rc; }

      node.info.numkeys = 1;
    }
  }

  //number of keys increases;
  return ERROR_NOERROR;
}

ERROR_T BTreeIndex::RangeQuery(const KEY_T &minkey, const KEY_T &maxkey,
                               list<VALUE_T> &valuelist)
{
  ERROR_T rc;
  BTreeNode leaf;

  KEY_T tempkey;
  SIZE_T leafptr;
  VALUE_T tempval;

  list<SIZE_T> clues;
  list<KEY_T> keylist;

  rc = LookupInsertion(clues, superblock.info.rootnode, minkey);
  if (rc) { return rc; }

  leafptr = clues.front();

  while (keylist.empty() || keylist.back() < maxkey) {
    rc = leaf.Unserialize(buffercache, leafptr);
    if (rc) { return rc; }

    for (SIZE_T i = 0; i < leaf.info.numkeys; i++) {
      rc = leaf.GetKey(i, tempkey);
      if (rc) { return rc; }

      if (minkey < tempkey && 
          tempkey < maxkey) {
        rc = leaf.GetVal(i, tempval);
        if (rc) { return rc; }

        keylist.push_back(tempkey);
        valuelist.push_back(tempval);
      }
    }

  //  if (tempkey.data > minkey.data) {
  //    break;
  //  }

    rc = leaf.GetPtr(0, leafptr);
    if (rc) { return rc; }
  }

  return ERROR_NOERROR;
}

ERROR_T BTreeIndex::Update(const KEY_T &key, const VALUE_T &value)
{
  VALUE_T tempval = value;
  return LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_UPDATE, key, tempval);
}

  
ERROR_T BTreeIndex::Delete(const KEY_T &key)
{
  // This is optional extra credit 
  //
  // 
  return ERROR_UNIMPL;
}

  
//
//
// DEPTH first traversal
// DOT is Depth + DOT format
//

ERROR_T BTreeIndex::DisplayInternal(const SIZE_T &node,
            ostream &o,
            BTreeDisplayType display_type) const
{
  KEY_T testkey;
  SIZE_T ptr;
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;

  rc= b.Unserialize(buffercache,node);

  if (rc!=ERROR_NOERROR) { 
    return rc;
  }

  rc = PrintNode(o,node,b,display_type);
  
  if (rc) { return rc; }

  if (display_type==BTREE_DEPTH_DOT) { 
    o << ";";
  }

  if (display_type!=BTREE_SORTED_KEYVAL) {
    o << endl;
  }

  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    if (b.info.numkeys>0) { 
      for (offset=0;offset<=b.info.numkeys;offset++) { 
  rc=b.GetPtr(offset,ptr);
  if (rc) { return rc; }
  if (display_type==BTREE_DEPTH_DOT) { 
    o << node << " -> "<<ptr<<";\n";
  }
  rc=DisplayInternal(ptr,o,display_type);
  if (rc) { return rc; }
      }
    }
    return ERROR_NOERROR;
    break;
  case BTREE_LEAF_NODE:
    return ERROR_NOERROR;
    break;
  default:
    if (display_type==BTREE_DEPTH_DOT) { 
    } else {
      o << "Unsupported Node Type " << b.info.nodetype ;
    }
    return ERROR_INSANE;
  }

  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::Display(ostream &o, BTreeDisplayType display_type) const
{
  ERROR_T rc;
  if (display_type==BTREE_DEPTH_DOT) { 
    o << "digraph tree { \n";
  }
  rc=DisplayInternal(superblock.info.rootnode,o,display_type);
  if (display_type==BTREE_DEPTH_DOT) { 
    o << "}\n";
  }
  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::SanityCheck() const
{
  set<SIZE_T> checked;
  list<KEY_T> leafkeys;
  ERROR_T rc;
  SIZE_T tempnode = superblock.info.rootnode;

  rc = Check(checked, leafkeys, tempnode);
  return rc;
}  

ERROR_T BTreeIndex::Check(set<SIZE_T> &checked, list<KEY_T> &leafkeys, SIZE_T &node) const
{
  BTreeNode b;
  ERROR_T rc;
  SIZE_T ptr;
  SIZE_T offset;
  KEY_T testkey;

  //
  //Check here to see if node has already been checked (by scanning the list)
  //check if there are inner loop of the node, if the same node been rechecked
  if (checked.count(node)) {
    return ERROR_INNERLOOP;
  } else {
    checked.insert(node);
  }


  rc = b.Unserialize(buffercache, node);
  if(rc) {return rc;}

  switch(b.info.nodetype){
    case BTREE_ROOT_NODE: {
      for(offset=0; offset<=b.info.numkeys; offset++){
        rc = b.GetPtr(offset, ptr);
        if(rc) {return rc;}

        rc = Check(checked, leafkeys, ptr);
        if (rc) { return rc; }
      }
      break;
    }
    case BTREE_INTERIOR_NODE: {
      if (b.info.numkeys > b.info.GetNumSlotsAsInterior()) {
        return ERROR_NODEOVERFLOW;
      }
    
      for(offset=0; offset<=b.info.numkeys; offset++){
        rc = b.GetPtr(offset, ptr);
        if(rc) {return rc;}

        rc = Check(checked, leafkeys, ptr);
        if (rc) { return rc; }
      }
    
      // Using iterator to test if it is sorted
      return ERROR_NOERROR;
      
      break;
    }

    case BTREE_LEAF_NODE: {
      if (b.info.numkeys > b.info.GetNumSlotsAsLeaf()) {
        return ERROR_NODEOVERFLOW;
      }
      
      for(offset = 0; offset < b.info.numkeys; offset++){
        rc = b.GetKey(offset,testkey);
        if(rc) {return rc;}

        leafkeys.push_back(testkey);
      }
      return ERROR_NOERROR;
      break;
    }

    default: {
      return ERROR_BADTYPE;
      break;
    }
  }
  while (!leafkeys.empty() && leafkeys.size() >= 2)
  {
    KEY_T testkey1 = leafkeys.front();
    leafkeys.pop_front();
    KEY_T testkey2 = leafkeys.front();
    if (testkey1.data < testkey2.data)
      return ERROR_BADORDER;
  }

  return ERROR_NOERROR;
}  


ostream & BTreeIndex::Print(ostream &os) const
{
  Display(os, BTREE_DEPTH_DOT);
  return os;
}
