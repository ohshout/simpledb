package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }

				public boolean equals(Object o) {
						if (!this.fieldType.equals(((TDItem) o).fieldType)) {
							return false;
						} else {
							String otherField = ((TDItem) o).fieldName;
							return this.fieldName == null ?
										 otherField == null :
										 this.fieldName.equals(otherField);
						}
				}
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
			return tuples.iterator();
    }

    private static final long serialVersionUID = 1L;

		private ArrayList<TDItem> tuples;
		private int len = 0;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
				tuples = new ArrayList<>();
				for (int i = 0; i < typeAr.length; i++) {
					tuples.add(new TDItem(typeAr[i], fieldAr[i]));
					len += typeAr[i].getLen();
				}
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
				tuples = new ArrayList<>();
				for (int i = 0; i < typeAr.length; i++) {
					tuples.add(new TDItem(typeAr[i], null));
					len += typeAr[i].getLen();
				}
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
				return tuples.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
				if (i >= 0 && i < numFields()) {
					return tuples.get(i).fieldName;
				} else {
					throw new NoSuchElementException();
				}
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
				if (i >= 0 && i < numFields()) {
					return tuples.get(i).fieldType;
				} else {
					throw new NoSuchElementException();
				}
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
				for (int i = 0; i < tuples.size(); i++) {
					if (getFieldName(i).equals(name)) {
						return i;
					}
				}

				throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
				return len;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
				int sz = td1.numFields() + td2.numFields();
				Type[] typeAr = new Type[sz];
				String[] fieldAr = new String[sz];

				Iterator<TDItem> it = td1.iterator();
				int idx = 0;

				while(it.hasNext()) {
					TDItem tdi = it.next();
					typeAr[idx] = tdi.fieldType;
					fieldAr[idx] = tdi.fieldName;
					idx++;
				}

				it = td2.iterator();

				while(it.hasNext()) {
					TDItem tdi = it.next();
					typeAr[idx] = tdi.fieldType;
					fieldAr[idx] = tdi.fieldName;
					idx++;
				}

				return new TupleDesc(typeAr, fieldAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
				if (this.numFields() != ((TupleDesc) o).numFields()) {
					return false;
				} else {
					Iterator<TDItem> it = ((TupleDesc) o).iterator();
					int idx = 0;

					while (it.hasNext()) {
						if (!this.tuples.get(idx).equals(((TDItem)it.next()))) {
							return false;
						}
						idx++;
					}

					return true;
				}
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
				String str = "";

				for (int i = 0; i < numFields(); i++) {
						TDItem tdi = tuples.get(i);
						str += tdi.fieldType.toString();
						str += "[" + i + "]" + "(";
						str += tdi.fieldName;
						str += "[" + i + "]" + "), ";
				}

				return str;
    }
}
