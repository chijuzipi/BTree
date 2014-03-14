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
	  // Write Me
	  rc = b.SetVal(offset,value);
	  if (rc) {return rc;}
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
		KEY_T instkey = key;//since during split and pop, the key been poped may change
		
		bool pop = true;
		BTreeNode b;
		ERROR_T rc;
		SIZE_T ptr;
		//first look up which leaf should insert to, and record clues
		rc = LookupInsertion(clues, superblock.info.rootnode, instkey);
		
		if(rc != ERROR_NOERROR) {return rc;}
		while (!clues.empty() && pop == true){
			rc= b.Unserialize(buffercache,clues.front());
			clues.pop_front(); // remove this node
			if (rc!=ERROR_NOERROR) {return rc;}
			
			InsertNode(b, instkey, value, pop, ptr); //only call InsertNode once
			
			rc = b.Serialize(buffercache, clues.front());
			if (rc != ERROR_NOERROR) {return rc;}

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

ERROR_T BTreeIndex::InsertNode(BTreeNode &b, KEY_T &key, const VALUE_T &value, bool &pop, SIZE_T &ptr)
{	
	ERROR_T rc;
	SIZE_T offset;
	KEY_T testkey;
	KEY_T temp_key;
	SIZE_T temp_ptr;
	KeyValuePair temp_pair;
	

	if (b.info.numkeys==0) {
		switch(b.info.nodetype) {
		
		case BTREE_ROOT_NODE: {
		
        //make two new leaf node to store the value, left and right 
		SIZE_T left_block; // 
        SIZE_T right_block;// right

        // Left leaf node
        rc = AllocateNode(left_block);
        if (rc) { return rc; }

        // Unserialize from block offset into left_node
        BTreeNode left_node;
        rc = left_node.Unserialize(buffercache,left_block);
        if (rc) { return rc; }
		
		//initialized left leaf node
        left_node.info.nodetype = BTREE_LEAF_NODE;
        left_node.info.numkeys = 0;
		
        rc = left_node.Serialize(buffercache,left_block);
        if (rc) { return rc; }

        // Right node
        rc = AllocateNode(right_block);
        if (rc) { return rc; }

        BTreeNode right_node;
        rc = right_node.Unserialize(buffercache,right_block);	
        if (rc) { return rc; }

        //initialized right leaf node
        right_node.info.nodetype = BTREE_LEAF_NODE;
        right_node.info.numkeys = 1;
        rc = right_node.SetKey(0,key);
        if (rc) { return rc; }
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

        // Set  pointer of root to point at blocks
        rc = b.SetPtr(0,left_block);
        if (rc) { return rc; }
        rc = b.SetPtr(1,right_block);
        if (rc) { return rc; }

        // No need to serialize root here
		// no need to pop, insertion is done
		pop = false;
        return ERROR_NOERROR;
		break;
		}
		
		case BTREE_INTERIOR_NODE: {
		//since no interior is allowed to be empty
		return ERROR_INSANE;
		break;
		}
		
		case BTREE_LEAF_NODE:{
		BTreeNode b;
		ERROR_T rc;
		SIZE_T offset;
		KEY_T testkey;
		SIZE_T ptr;
		KEY_T temp_key;
		VALUE_T temp_val;


		// if there is room in the leaf, insert it in the right place
		if (b.info.numkeys>0 && b.info.numkeys<b.info.GetNumSlotsAsInterior()) {
			for(offset = 0; offset<b.info.numkeys;offset++){
				// Move through keys until we find one larger than input key
				rc = b.GetKey(offset,testkey);
				if (rc) { return rc; }
				// If key exists, conflict error.
		    if (key == testkey) { return ERROR_CONFLICT; }
				// Once one is found, break loop
				if (key<testkey) { break; }
			}
			for (int i=b.info.numkeys-2; i>=offset; i--) {
				// Shift key
				rc = b.GetKey(i,temp_key);
				if (rc) { return rc; }
				rc = b.SetKey(i+1,temp_key);
				if (rc) { return rc; }

				//Shift value
				rc = b.GetVal(i+1,temp_val);
				if (rc) { return rc; }
				rc = b.SetVal(i+2,temp_val);
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
				rc = b.Serialize(buffercache,ptr);
				if (rc) { return rc; }
				
				// no need to pop, insert is done
				pop = false;

				return ERROR_NOERROR;
		}
	}
		
		default:
		//no other type of node is allowed for the tree
		return ERROR_INSANE;
		
		}
	}
	
	if (b.info.numkeys>0 && b.info.numkeys<b.info.GetNumSlotsAsInterior()) {
		

		for(offset = 0; offset<b.info.numkeys;offset++){
			// Move through keys until we find one larger than input key
			rc = b.GetKey(offset,testkey);
			if (rc) { return rc; }
			// If key exists, conflict error.
			if (key == testkey) { return ERROR_CONFLICT;}
			// Otherwise, break loop
			if (key<testkey) { break; }
		}
		for (unsigned int i=b.info.numkeys-2; i>=offset; i--) {

			switch(b.info.nodetype) {
			case BTREE_ROOT_NODE:
			case BTREE_INTERIOR_NODE: {
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
			break;
			}
			//shift lef
			case BTREE_LEAF_NODE: {
			rc = b.GetKeyVal(i, temp_pair);
			if (rc) { return rc; }
			rc = b.SetKeyVal(i+1, temp_pair);
			break;
			}
			default:
			return ERROR_INSANE;
			}
		}
		
			// Set input key
			rc = b.SetKey(offset,key);
			if (rc) { return rc; }

			// Set input ptr
			if(b.info.nodetype == BTREE_LEAF_NODE){
				rc = b.SetVal(offset,value);
				if (rc) { return rc; }
			}
			if(b.info.nodetype == BTREE_ROOT_NODE or b.info.nodetype == BTREE_INTERIOR_NODE){
				rc = b.SetPtr(offset+1,ptr);
				if (rc) { return rc; }
			}
			
			//number of keys increases;
			b.info.numkeys++;

			// no need to pop, insert is done
			pop = false;
			return ERROR_NOERROR;
			
	}
	
	//when the node is full, need to split and pop
	///
	// if(b.info.numkeys = b.info.GetNumSlotsAsInterior()) {
		
		// switch(b.info.nodetype) {
		// case BTREE_ROOT_NODE:
		// *p = new char [b.info.GetNumDataBytes()+sizeof(SIZE_T)+sizeof(KEY_T)];
		
		// SIZE_T new_block;
		// rc = AllocateNode(new_block);
        // if (rc) {return rc;}
		// BTreeNode new_node; // the split get a new node
		// rc = new_node.Unserialize(buffercache,new_block);
		// if (rc) {return rc;}
		// for(offset = 0; offset<b.info.numkeys;offset++){
			// allocate();
			// allocate();
			// pop = false;
		// }
		
		// break;
		// case BTREE_INTERIOR_NODE:
		// break;
		// case BTREE_LEAF_NODE:
		// break;
		// default:
		// return ERROR_INSANE
		// }
	// }	
}

  
ERROR_T BTreeIndex::Update(const KEY_T &key, VALUE_T &value)
{
  return LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_UPDATE, key, value);
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
	rc = Check(checked, leafkeys, superblock.info.rootnode);
	return rc;

}  

ERROR_T BTreeIndex::Check(set<SIZE_T> &checked, list<KEY_T> &leafkeys, const SIZE_T &node) const
{
	BTreeNode b;
	ERROR_T rc;
	SIZE_T ptr;
	SIZE_T offset;
	KEY_T testkey;
	KEY_T testkey1;
	KEY_T testkey2;

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
	case BTREE_ROOT_NODE:
	case BTREE_INTERIOR_NODE:

	if (b.info.numkeys >= b.info.GetNumSlotsAsInterior()) {
	  return ERROR_NODEOVERFLOW;
	}

	for(offset=0; offset<=b.info.numkeys; offset++){
	  rc = b.GetPtr(offset, ptr);
	  if(rc) {return rc;}
	  rc = Check(checked, leafkeys, ptr);
	  if (rc) { return rc; }
	}
	//test if leafkeys is sorted
	for(unsigned int i=0; i < leafkeys.size(); i++)
	{
		testkey1 = leafkeys.front();
		leafkeys.pop_front();
		testkey2 = leafkeys.front();
		leafkeys.pop_front();
		if (testkey1.data > testkey2.data)
		return ERROR_BADORDER;
	}
	return ERROR_NOERROR;
	
	break;

	case BTREE_LEAF_NODE:
	if (b.info.numkeys >= b.info.GetNumSlotsAsLeaf()) {
	  return ERROR_NODEOVERFLOW;
	}
	
	for(offset=0; offset<b.info.numkeys; offset++){
		rc = b.GetKey(offset,testkey);
		if(rc) {return rc;}
		leafkeys.push_back(testkey);
	}
	return ERROR_NOERROR;
	break;
	default:
	return ERROR_BADTYPE;
	break;
	}
	return ERROR_INSANE;
}	


ostream & BTreeIndex::Print(ostream &os) const
{
  Display(os, BTREE_DEPTH_DOT);
  return os;
}
