#include <assert.h>
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
      if (key<testkey || key==testkey) {
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
	  // BTREE_OP_UPDATE
	  b.SetVal(offset,value);
	  return ERROR_UNIMPL;
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
		KEY_T element = key;//since during split and pop, the key been poped may change
		
		bool pop = true;
		BTreeNode b;
		ERROR_T rc;
		SIZE_T ptr;
		//first look up which leaf should insert to, and record clues
		rc = LookupInsertion(clues, superblock.info.rootnode, element);
		
		if(rc != ERROR_NONERROR) {return rc;}
		while (!clues.empty() && pop == true){
			rc= b.Unserialize(buffercache,clues.front());
			clues.pop_front();
			if (rc!=ERROR_NOERROR) {return rc;}
			
			switch(b.info.nodetype) {
			case BTREE_ROOT_NODE:
				InsertRoot(b, element, value, pop, ptr);
				
			case BTREE_INTERIOR_NODE:
				InsertInterior(b, element, pop, ptr);// interior only insert key
			case BTREE_LEAF_NODE:
				InsertLeaf(b, element, value, pop, ptr);
			}
			
			rc = b.serialize(buffercache, clues.front());
			if (rc != ERROR_NONERROR) {return rc;}

		}
}

ERROR_T BTreeIndex::LookupInsertion(list<SIZE_T> &clues, const SIZE_T &node, const KEY_T &key)					   
{
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;
  KEY_T testkey;
  SIZE_T ptr;
  clues.push_front(node);

  rc= b.Unserialize(buffercache,node);

  if (rc!=ERROR_NOERROR) { 
    return rc;
  }

  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
  
    for (offset=0;offset<b.info.numkeys;offset++) { 
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (key<testkey) {

	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	return LookupInsertion(clues, ptr, key);
      }
    }

	//the scan goes to the last key in the node (return the point of the right)
    if (b.info.numkeys>0) { 
      rc=b.GetPtr(b.info.numkeys,ptr);
      if (rc) { return rc; }
      return LookupInsertion(clues, ptr, key);
    } else {
      // the node is empty
      return ERROR_NOERROR;
    }
    break;
  case BTREE_LEAF_NODE:
    // direct return the node pointer
      return ERROR_NOERROR;
  }
}

ERROR_T BTreeIndex::InsertRoot(BTreeNode &b, KEY_T &element, const VALUE_T &value, bool &pop, SIZE_T &ptr)
{	
	ERROR_T rc;
	SIZE_T offset;
	KEY_T testkey;
	KEY_T temp_key;
	SIZE_T temp_ptr;

	if (b.info.numkeys==0) {
        //
        // Special case where rootnode is empty  

        SIZE_T left_block; //block number of the new left leaf block
        SIZE_T right_block;// right

        // Left node
        //
        // Get block offset from AllocateNode
        rc = AllocateNode(left_block);
        if (rc) { return rc; }

        // Unserialize from block offset into left_node
        BTreeNode left_node;
        rc = left_node.Unserialize(buffercache,left_block);//put the left block into buffer, if it's not superblock, only copy the info session
        if (rc) { return rc; }

        // the new left_node is a leaf node
		// 
        left_node.info.nodetype = BTREE_LEAF_NODE;
        left_node.data = new char [left_node.info.GetNumDataBytes()];//address of the new string 
        memset(left_node.data,0,left_node.info.GetNumDataBytes()); //set the new string to be 0

        // Set number of keys in left_node to 0
        left_node.info.numkeys = 0;

        // Serialize left_node back into buffer
        rc = left_node.Serialize(buffercache,left_block);
        if (rc) { return rc; }


        // Right node
        //
        // Get block offset from AllocateNode
        rc = AllocateNode(right_block);
        if (rc) { cout<<rc<<endl; return rc; }

        // Unserialize from block offset into right_node
        BTreeNode right_node;
        rc = right_node.Unserialize(buffercache,right_block);	
        if (rc) { return rc; }

        // right_node is a leaf node
        right_node.info.nodetype = BTREE_LEAF_NODE;
        right_node.data = new char [right_node.info.GetNumDataBytes()];
        memset(right_node.data,0,right_node.info.GetNumDataBytes());

        // Set number of keys in right_node to 1
        right_node.info.numkeys = 1;

        // Set key of right_node
        rc = right_node.SetKey(0,key);
        if (rc) { return rc; }

        // Set value in right_node
        rc = right_node.SetVal(0,value);
        if (rc) { return rc; }

        // Serialize right_node back into buffer
        rc = right_node.Serialize(buffercache,right_block);
        if (rc) { return rc; }


        // Root node
        //
        // Set number of keys in root to 1
        b.info.numkeys = 1;

        // Set key in root
        rc = b.SetKey(0,key);
        if (rc) { return rc; }

        // Set left pointer of root to point at left_node
        rc = b.SetPtr(0,left_block);
        if (rc) { return rc; }

        // Set right pointer of root to point at right_node
        rc = b.SetPtr(1,right_block);
        if (rc) { return rc; }

        // Serialize root node, not need, in the caller function
        //rc = b.Serialize(buffercache,node);
        if (rc) { return rc; }	
		
		// no need to pop, insertion is done
		pop = false;

        return ERROR_NOERROR;
		
	} 
	
	if (b.info.numkeys>0 && b.info.numkeys<b.info.GetNumSlotsAsInterior()) {
		for(offset = 0; offset<b.info.numkeys;offset++){
			// Move through keys until we find one larger than input key
			rc = b.GetKey(offset,testkey);
			if (rc) { return rc; }
			// If key exists, conflict error.
			if (key == testkey) { return ERROR_CONFLICT; }
			// Otherwise, break loop
			if (key<testkey) { break; }
		}
		for (int i=b.info.numkeys-2; i>=offset; i--) {
			
			// Shift key
			rc = b.GetKey(i,temp_key);
			if (rc) { return rc; }
			rc = b.SetKey(i+1,temp_key);
			if (rc) { return rc; }

			//Shift pointer
			rc = b.GetPtr(i+1,temp_ptr);
			if (rc) { return rc; }
			rc = b.SetPtr(i+2,temp_ptr);
			if (rc) { return rc; }
		}
		
			// Set input key
			rc = b.SetKey(offset,key);
			if (rc) { return rc; }

			// Set input ptr
			rc = b.SetPtr(offset+1,ptr);
			if (rc) { return rc; }
			//number of keys increases;
			b.info.numkeys++;

			// Serialize block
			rc = b.Serialize(buffercache,node);
			if (rc) { return rc; }
			
			// no need to pop, insert is done
			pop = false;

			return ERROR_NOERROR;
	}
	
	// the special case that need to split the root
	////not yet finish 
	///
	if(b.info.numkeys = b.info.GetNumSlotsAsInterior()) {
		
		*p = new char [b.info.GetNumDataBytes()+sizeof(SIZE_T)+sizeof(KEY_T)];
		
		SIZE_T new_block;
		rc = AllocateNode(new_block);
        if (rc) {return rc;}
		BTreeNode new_node; // the split get a new node
		rc = new_node.Unserialize(buffercache,new_block);
		if (rc) {return rc;}
		for(offset = 0; offset<b.info.numkeys;offset++){
			
		
		allocate();
		allocate();
		pop = false;
		}
	}	
}




 
ERROR_T BTreeIndex::InsertLeaf(const SIZE_T &node, KEY_T &key, const VALUE_T &value, bool &pop, SIZE_T &pointer) 
{
	BTreeNode b;
	ERROR_T rc;
	SIZE_T offset;
	KEY_T testkey;
	SIZE_T ptr;


	// if there is room in the leaf, insert it in the right place
	if (b.info.numkeys>0 && b.info.numkeys<b.info.GetNumSlotsAsInterior()) {
		for(offset = 0; offset<b.info.numkeys;offset++){
			// Move through keys until we find one larger than input key
			rc = b.GetKey(offset,testkey);
			if (rc) { return rc; }
			// Once one is found, break loop
			if (key<testkey) { break; }
		}
		for (int i=offset; i<b.info.numkeys; i++) {
			// Shift key
			rc = b.GetKey(i,temp_key_ref);
			if (rc) { return rc; }
			rc = b.SetKey(i+1,temp_key_ref);
			if (rc) { return rc; }

			//Shift value
			rc = b.GetVal(i+1,temp_val_ref);
			if (rc) { return rc; }
			rc = b.SetVal(i+2,temp_val_ref);
			if (rc) { return rc; }
		}
		
			// Set input key
			rc = b.SetKey(offset,key);
			if (rc) { return rc; }

			// Set input val
			rc = b.SetVal(offset+1,value);
			if (rc) { return rc; }
			//number of keys increases;
			b.info.numkeys++;

			// Serialize block
			rc = b.Serialize(buffercache,node);
			if (rc) { return rc; }
			
			// no need to pop, insert is done
			pop = false;

			return ERROR_NOERROR;
	}

	if (b.info.numkeys == b.info.GetNumSlotsAsInterior()){

		//find middle value (mid_value) of the keys in the block + the new key
		//temp is a temporary array to hold these
		const keynum = b.info.GetNumSlotsAsInterior()+1;
		KEY_T temp [keynum];
		// Move through keys until we find one larger than input key 
		//and input them into the temp array
		for(offset = 0; offset<keynum;offset++){
			rc = b.GetKey(offset,testkey);
			if (rc) { return rc; }
			// If key exists, conflict error.
			if (key == testkey) { return ERROR_CONFLICT; }
			// If fetched key is less, put it into the temp array
			if (key>testkey) {
				temp[offset] = testkey;
			}
			// Otherwise, insert the key and break loop
			if (key<testkey) { 
				temp[offset] = key;
				break; }
		}
		//Now insert the rest of the node keys into the temp array
		for (int i=offset+1; i<keynum; i++) {
			rc = b.GetKey(i-1,testkey);
			if (rc) { return rc; }
			temp[i] = testkey;
		}

		//Now find the offset of the middle value of the temp array
		mid_value = (keynum/2) + 1;

		//allocate a new block for the new leaf
		SIZE_T new_block_loc; //block number of the new block
        SIZE_T& new_block_ref = new_block_loc;

        // Get block offset from AllocateNode
        rc = AllocateNode(new_block_ref);
        if (rc) { cout<<rc<<endl; return rc; }

        // Unserialize from block offset into new_node
        BTreeNode new_node;
        rc = new_node.Unserialize(buffercache,new_block_loc);	//put the new block into buffer
        if (rc) { return rc; }

        new_node.info.nodetype = BTREE_LEAF_NODE;
        new_node.data = new char [new_node.info.GetNumDataBytes()];


        //split into two nodes here

        

		//put the two nodes back into memory
        //new node:
        rc = new_node.Serialize(buffercache,new_block_loc);
        if (rc) { return rc; }
        //old node:
        rc = b.Serialize(buffercache,node);
        if (rc) { return rc; }
        
        //pointer = number of new node SET THIS BEFORE RETURN
        //don't forget to set key to pass here
        pointer = new_block_loc;
		pop = true;
	}
}


{
  // WRITE ME
  return ERROR_UNIMPL;
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
	KEY_T testkey;
	set<SIZE_T> checked;
	set<KEY_T> leafkeys;
	ERROR_T rc;
	rc = Check(checked, leafkeys, superblock.info.rootnode);
	return rc;

}  

ERROR_T BTreeIndex::Check(set<SIZE_T> &Checked, set<KEY_T> &leafkeys, const SIZE_T &node) const
{
	BTreeNode b;
	ERROR_T rc;
	SIZE_T ptr;
	SIZE_T offset;

	//
	//Check here to see if node has already been checked (by scanning the visited list)
	//check if there are inner loop of the node:
	if (checked.count(node)) {
	return ERROR_INNERLOOP;
	} else {
	// Haven't seen this. Insert it.
	checked.insert(node);
	}


	rc = b.Unserialize(buffercache, node);
	if(rc) {return rc;}

	switch(b.info.nodetype){
	case BTREE_ROOT_NODE:
	case BTREE_INTERIOR_NODE:

	if (b.info.numkeys >= b.info.GetNumSlotsAsInterior()) {
	  return ERROR_TOOMANYELEMENTS;
	}

	for(offset=0; offset<=b.info.numkeys; offset++){
	  rc = b.GetPtr(offset, ptr);
	  if(rc) {return rc;}
	  rc = Check(Checked, ptr);
	  if (rc) { return rc; }
	}
	//using iterator to test if it is sorted
	for(std::set<KEY_T>::iterator it=leafkeys.begin(); it!=leafkeys.end(); ++it)
	{
		if(*it > *(it++))
		{return ERROR_BADORDER;}
	}
	return ERROR_NOERROR;
	
	break;

	case BTREE_LEAF_NODE:
	if (b.info.numkeys >= b.info.GetNumSlotsAsLeaf()) {
	  return ERROR_TOOMANYELEMENTS;
	}
	for(offset=0; offset<=b.info.numkeys; offset++){
		rc = b.GetKey(offset,testkey);
		if(rc) {return rc;}
		leafkeys.insert(testkey);
	}
	return ERROR_NOERROR;
	break;
	default:
	return ERROR_INSANE;
	break;
	}
	return ERROR_INSANE;
}	


ostream & BTreeIndex::Print(ostream &os) const
{
  Display(os, BTREE_DEPTH_DOT);
  return os;
}




