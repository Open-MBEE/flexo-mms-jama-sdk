export type Iri = `http${'s' | ''}://${string}`;

export interface BindingIri {
	type: 'uri';
	value: Iri;
}

export interface BindingLiteral<s_value extends string=string> {
	type: 'literal';
	value: s_value;
	datatype?: Iri;
	'xml:lang'?: string;
}

export type GenericBindingRow = Record<string, BindingIri | BindingLiteral>[];

export interface UserRow {
	user: BindingIri;
	username: BindingLiteral;
	firstName: BindingLiteral;
	lastName: BindingLiteral;
	email?: BindingLiteral;
}

export interface ProjectRow {
	projectFields: BindingIri;
	projectName: BindingLiteral;
	projectKey: BindingLiteral;
	projectDescription: BindingLiteral;
	manager?: BindingIri;
	managerFirstName?: BindingLiteral;
	managerLastName?: BindingLiteral;
	managerEmail?: BindingLiteral;
}

export interface ItemTypeFieldRow {
	itemType: BindingIri;
	field: BindingIri;
	fieldName: BindingLiteral;
	fieldType: BindingLiteral;
	fieldLabel: BindingLiteral;
	picklistId?: BindingLiteral;
}

export interface PicklistRow {
	picklist: BindingIri;
	picklistId: BindingLiteral;
	picklistName: BindingLiteral;
	picklistDescription: BindingLiteral;
}

export interface PicklistOptionRow {
	picklist: BindingIri;
	option: BindingIri;
	optionId: BindingLiteral;
	optionName: BindingLiteral;
	optionaDescription: BindingLiteral;
	optionValue: BindingLiteral;
}

export interface ItemFieldPropertyRow {
	item: BindingIri;
	property: BindingIri;
	value: BindingIri | BindingLiteral;
}

export interface ItemRelationshipRow {
	relation: BindingIri;
	relationName: BindingLiteral;
	src: BindingIri;
	dst: BindingIri;
}

export interface ItemRow {
	item: BindingIri;
	itemType: BindingIri;
	itemTypeDisplay: BindingLiteral;
	itemKey: BindingLiteral;
	itemName: BindingLiteral;
	createdBy: BindingIri;
	modifiedBy: BindingIri;
	createdDate: BindingLiteral;
	modifiedDate: BindingLiteral;
	lastActivityDate: BindingLiteral;
	parent?: BindingIri;
}

export interface UserRow {
	user: BindingIri;
	username: BindingLiteral;
	firstName: BindingLiteral;
	lastName: BindingLiteral;
	email?: BindingLiteral;
}
