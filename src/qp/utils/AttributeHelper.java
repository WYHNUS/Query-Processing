package qp.utils;

public class AttributeHelper {
    final Attribute attribute;
    final OrderByEnum order;
    /**
     * To know the index of the attribute. Used directly in the Sort Algorithm.
     */
    int attributeIndexInSchema;

    /**
     * ASC by default.
     * 
     * @param attribute
     */
    public AttributeHelper(Attribute attribute) {
	this.attribute = attribute;
	this.order = OrderByEnum.ASC;
    }

    /**
     * @param attribute
     * @param order
     */
    public AttributeHelper(Attribute attribute, OrderByEnum order) {
	this.attribute = attribute;
	this.order = order;
    }

    /**
     * @return the attribute
     */
    public Attribute getAttribute() {
	return attribute;
    }

    /**
     * @return the order
     */
    public OrderByEnum getorder() {
	return order;
    }

    /**
     * @return the attributeIndexInSchema
     */
    public int getAttributeIndexInSchema() {
        return attributeIndexInSchema;
    }

    /**
     * @param attributeIndexInSchema the attributeIndexInSchema to set
     */
    public void setAttributeIndexInSchema(int attributeIndexInSchema) {
        this.attributeIndexInSchema = attributeIndexInSchema;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#clone()
     */
    public AttributeHelper clone() {
	return new AttributeHelper(attribute, order);
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result + ((attribute == null) ? 0 : attribute.hashCode());
	result = prime * result + ((order == null) ? 0 : order.hashCode());
	return result;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
	if (this == obj)
	    return true;
	if (obj == null)
	    return false;
	if (getClass() != obj.getClass())
	    return false;
	AttributeHelper other = (AttributeHelper) obj;
	if (attribute == null) {
	    if (other.attribute != null)
		return false;
	} else if (!attribute.equals(other.attribute))
	    return false;
	if (order == null) {
	    if (other.order != null)
		return false;
	} else if (!order.equals(other.order))
	    return false;
	return true;
    }
    
    
}
