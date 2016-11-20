/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import uuid from 'node-uuid';

const SCHEMA_TYPES = {
  'types': [
    'boolean',
    'bytes',
    'double',
    'float',
    'int',
    'long',
    'string',
    'array',
    'enum',
    'map',
    'union',
    'record'
  ],
  'simpleTypes': [
    'boolean',
    'bytes',
    'double',
    'float',
    'int',
    'long',
    'string',
  ]
};

function parseType(type) {
  let storedType = type;
  let nullable = false;

  if (type.getTypeName() === 'union:wrapped') {
    type = type.getTypes();

    if (type[1] && type[1].getTypeName() === 'null') {
      storedType = type[0];
      type = type[0].getTypeName();
      nullable = true;
    } else {
      type = 'union';
    }
  } else {
    type = type.getTypeName();
  }

  return {
    displayType: type,
    type: storedType,
    nullable: nullable,
    nested: checkComplexType(type)
  };
}
function getParsedSchema(schema) {
  let defaultSchema = {
    name: '',
    type: 'string',
    displayType: 'string',
    nullable: false,
    id: 'a' + uuid.v4().split('-').join(''),
    nested: false
  };
  const isEmptySchema = () => {
    if (!schema && !(schema.fields || (schema.getFields && schema.getFields().length > 0))) {
      return true;
    }
    return false;
  };

  if (isEmptySchema(schema) || (!schema || schema === 'record')) {
    return defaultSchema;
  }
  let parsed;

  try {
    parsed = avsc.parse(schema, { wrapUnions: true });
  } catch(e) {
    return;
  }

  let parsedSchema = parsed.getFields().map((field) => {
    let type = field.getType();

    let partialObj = parseType(type);

    return Object.assign({}, partialObj, {
      id: 'a' + uuid.v4().split('-').join(''),
      name: field.getName()
    });

  });

  return parsedSchema;
}

function checkComplexType(displayType) {
  let complexTypes = ['array', 'enum', 'map', 'record', 'union'];
  return complexTypes.indexOf(displayType) !== -1 ? true : false;
}

export {
  SCHEMA_TYPES,
  parseType,
  checkComplexType,
  getParsedSchema
};
