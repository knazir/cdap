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

import React, {PropTypes, Component} from 'react';
import SelectWithOptions from 'components/SelectWithOptions';
import {parseType, SCHEMA_TYPES, checkComplexType, checkParsedTypeForError} from 'components/SchemaEditor/SchemaHelpers';
import AbstractSchemaRow from 'components/SchemaEditor/AbstractSchemaRow';
import {Input} from 'reactstrap';
import classnames from 'classnames';

require('./MapSchemaRow.less');

export default class MapSchemaRow extends Component {
  constructor(props) {
    super(props);
    if (typeof props.row === 'object') {
      let rowType = parseType(props.row);
      this.state = {
        name: rowType.name,
        keysType: rowType.type.getKeysType().getTypeName(),
        keysTypeNullable: rowType.nullable,
        valuesType: rowType.type.getValuesType().getTypeName(),
        valuesTypeNullable: rowType.nullable,
        error: ''
      };
      this.parsedKeysType = rowType.type.getKeysType();
      this.parsedValuesType = rowType.type.getValuesType();
    } else {
      this.state = {
        name: props.row.name,
        keysType: 'string',
        keysTypeNullable: false,
        valuesType: 'string',
        valuesTypeNullable: false,
        error: ''
      };
      this.parsedKeysType = 'string';
      this.parsedValuesType = 'string';
    }
    setTimeout(this.updateParent.bind(this));
    this.onKeysTypeChange = this.onKeysTypeChange.bind(this);
    this.onValuesTypeChange = this.onValuesTypeChange.bind(this);
  }
  onKeysTypeChange(e) {
    let error;
    if (SCHEMA_TYPES.simpleTypes.indexOf(e.target.value) !== -1) {
      error = checkParsedTypeForError(e.target.value);
      if (error) {
        this.setState({ error });
        return;
      }
    }
    this.parsedKeysType = e.target.value;
    this.setState({
      keysType: e.target.value
    }, this.updateParent.bind(this));
  }
  onValuesTypeChange(e) {
    let error;
    if (SCHEMA_TYPES.simpleTypes.indexOf(e.target.value) !== -1) {
      error = checkParsedTypeForError(e.target.value);
      if (error) {
        this.setState({
          error
        });
        return;
      }
    }
    this.parsedValuesType = e.target.value;
    this.setState({
      valuesType: e.target.value
    }, this.updateParent.bind(this));
  }
  updateParent() {
    if (checkParsedTypeForError(this.parsedValuesType) || checkParsedTypeForError(this.parsedKeysType)) {
      return;
    }
    this.props.onChange({
      type: 'map',
      keys: this.state.keysTypeNullable ? [this.parsedKeysType, null] : this.parsedKeysType,
      values: this.state.valuesTypeNullable ? [this.parsedValuesType, null] : this.parsedValuesType
    });
  }
  onKeysChildrenChange(keysState) {
    if (checkParsedTypeForError(keysState)) {
      return;
    }
    this.parsedKeysType = keysState;
    this.updateParent();
  }
  onValuesChildrenChange(valuesState) {
    if (checkParsedTypeForError(valuesState)) {
      return;
    }
    this.parsedValuesType = valuesState;
    this.updateParent();
  }
  onKeysTypeNullableChange(e) {
    this.setState({
      keysTypeNullable: e.target.checked
    }, this.updateParent.bind(this));
  }
  onValuesTypeNullableChange(e) {
    this.setState({
      valuesTypeNullable: e.target.checked
    }, this.updateParent.bind(this));
  }
  render() {
    return (
      <div className="map-schema-row">
        <div className="text-danger">
          {this.state.error}
        </div>
        <div className="schema-row">
          <div className={
            classnames("key-row clearfix", {
              "nested": checkComplexType(this.state.keysType)
            })
          }>
            <div className="field-name">
              <div> Key: </div>
              <SelectWithOptions
                options={SCHEMA_TYPES.types}
                value={this.state.keysType}
                onChange={this.onKeysTypeChange}
              />
            </div>
            <div className="field-type"></div>
            <div className="field-isnull pull-right">
              <div className="btn btn-link">
                <Input
                  type="checkbox"
                  value={this.state.keysTypeNullable}
                  onChange={this.onKeysTypeNullableChange.bind(this)}
                />
              </div>
            </div>
            {
              checkComplexType(this.state.keysType) ?
                <AbstractSchemaRow
                  row={this.parsedKeysType}
                  onChange={this.onKeysChildrenChange.bind(this)}
                />
              :
                null
            }
          </div>
          <div className={
            classnames("value-row clearfix", {
              "nested": checkComplexType(this.state.valuesType)
            })
          }>
            <div className="field-name">
              <div>Value: </div>
              <SelectWithOptions
                options={SCHEMA_TYPES.types}
                value={this.state.valuesType}
                onChange={this.onValuesTypeChange}
              />
            </div>
            <div className="field-type"></div>
            <div className="field-isnull pull-right">
              <div className="btn btn-link">
                <Input
                  type="checkbox"
                  value={this.state.valuesTypeNullable}
                  onChange={this.onValuesTypeNullableChange.bind(this)}
                />
              </div>
            </div>
              {
                checkComplexType(this.state.valuesType) ?
                  <AbstractSchemaRow
                    row={this.parsedValuesType}
                    onChange={this.onValuesChildrenChange.bind(this)}
                  />
                :
                  null
              }
          </div>
        </div>
      </div>
    );
  }
}

MapSchemaRow.propTypes = {
  row: PropTypes.any,
  onChange: PropTypes.func.isRequired
};
