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
import {parseType, SCHEMA_TYPES, checkComplexType, checkParsedTypeForError} from 'components/SchemaEditor/SchemaHelpers';
import SelectWithOptions from 'components/SelectWithOptions';
import AbstractSchemaRow from 'components/SchemaEditor/AbstractSchemaRow';
import {Input} from 'reactstrap';
import {insertAt, removeAt} from 'services/helpers';
import uuid from 'node-uuid';
import classnames from 'classnames';
require('./UnionSchemaRow.less');

export default class UnionSchemaRow extends Component {
  constructor(props) {
    super(props);
    if (typeof props.row === 'object') {
      let parsedTypes = props.row.getTypes();
      let displayTypes = parsedTypes.map(type => Object.assign({}, parseType(type), {id: 'a' + uuid.v4()}));
      this.state = {
        displayTypes,
        error: ''
      };
      this.parsedTypes = parsedTypes;
    } else {
      this.state = {
        displayTypes: [
          {
            displayType: 'string',
            type: 'string',
            id: uuid.v4(),
            nullable: false
          }
        ],
        error: ''
      };
      this.parsedTypes = [
        'string'
      ];
    }
    setTimeout(this.props.onChange.bind(this, this.parsedTypes));
    this.onTypeChange = this.onTypeChange.bind(this);
  }
  onNullableChange(index, e) {
    let displayTypes = this.state.displayTypes;
    let parsedTypes = this.parsedTypes;
    displayTypes[index].nullable = e.target.checked;
    let error = '';
    if (e.target.checked) {
      parsedTypes[index] = [
        parsedTypes[index],
        null
      ];
    } else {
      parsedTypes[index] = parsedTypes[index][0];
    }
    this.parsedTypes = parsedTypes;
    this.setState({
      displayTypes,
      error
    }, () => {
      this.props.onChange(this.parsedTypes);
    });
  }
  onTypeChange(index, e) {
    let displayTypes = this.state.displayTypes;
    displayTypes[index].displayType = e.target.value;
    displayTypes[index].type = e.target.value;
    let parsedTypes = this.parsedTypes;
    let error = '';
    if (displayTypes[index].nullable) {
      parsedTypes[index] = [
        e.target.value,
        null
      ];
    } else {
      parsedTypes[index] = e.target.value;
    }
    if (SCHEMA_TYPES.simpleTypes.indexOf(e.target.value) !== -1) {
      error = checkParsedTypeForError(parsedTypes);
      if (error) {
        this.setState({ error });
        return;
      }
    }
    this.parsedTypes = parsedTypes;
    this.setState({
      displayTypes,
      error
    }, this.props.onChange.bind(this, this.parsedTypes));
  }
  onTypeAdd(index) {
    let displayTypes = insertAt([...this.state.displayTypes], index, {
      type: 'string',
      displayType: 'string',
      id: uuid.v4(),
      nullable: false
    });
    let parsedTypes = insertAt([...this.parsedTypes], index, 'string');
    this.parsedTypes = parsedTypes;
    this.setState({ displayTypes }, this.props.onChange.bind(this, this.parsedTypes));
  }
  onTypeRemove(index) {
    let displayTypes = removeAt([...this.state.displayTypes], index);
    let parsedTypes = removeAt([...this.parsedTypes], index);
    this.parsedTypes = parsedTypes;
    this.setState({ displayTypes }, this.props.onChange.bind(this, this.parsedTypes));
  }
  onChildrenChange(index, parsedType) {
    let parsedTypes = this.parsedTypes;
    let displayTypes = this.state.displayTypes;
    let error;
    if (displayTypes[index].nullable) {
      parsedTypes[index] = [
        parsedType,
        null
      ];
    } else {
      parsedTypes[index] = parsedType;
    }
    error = checkParsedTypeForError(parsedTypes);
    if (error) {
      return;
    }
    this.parsedTypes = parsedTypes;
    this.props.onChange(this.parsedTypes);
  }
  render() {
    return (
      <div className="union-schema-row">
        <div className="text-danger">
          {this.state.error}
        </div>
          {
            this.state.displayTypes.map((displayType, index) => {
              return (
                <div
                  className={
                    classnames("schema-row clearfix", {
                      "nested": checkComplexType(displayType.displayType)
                    })
                  }
                  key={displayType.id}
                >
                  <div className="field-name">
                    <SelectWithOptions
                      options={SCHEMA_TYPES.types}
                      value={displayType.displayType}
                      onChange={this.onTypeChange.bind(this, index)}
                    />
                  </div>
                  <div className="field-isnull pull-right">
                    <div className="btn btn-link">
                      <Input
                        type="checkbox"
                        value={displayType.nullable}
                        onChange={this.onNullableChange.bind(this, index)}
                      />
                    </div>
                    <div className="btn btn-link">
                      <span
                        className="fa fa-plus"
                        onClick={this.onTypeAdd.bind(this, index)}
                      ></span>
                    </div>
                    <div className="btn btn-link">
                      {
                        this.state.displayTypes.length !== 1 ?
                          <span
                            className="fa fa-trash fa-xs text-danger"
                            onClick={this.onTypeRemove.bind(this, index)}
                          >
                          </span>
                        :
                          null
                      }
                    </div>
                  </div>
                  {
                    checkComplexType(displayType.displayType) ?
                      <AbstractSchemaRow
                        row={displayType.type}
                        onChange={this.onChildrenChange.bind(this, index)}
                      />
                    :
                      null
                  }
                </div>
              );
            })
          }
      </div>
    );
  }
}
UnionSchemaRow.propTypes = {
  row: PropTypes.any,
  onChange: PropTypes.func.isRequired
};
