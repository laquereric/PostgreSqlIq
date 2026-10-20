# The domain's second hop: a token's status, and nothing else, from the domain's store to the proxy's routing store
# (UC_DataPlatform RegistryMove.md, slice 3). It has its own slot, so neither hop waits on the other, and the proxy
# puts what arrives as values on its plane, so its routes are never truncated. Here the domain is the run's scratch
# Postgres (a real slot), the IQ Server the bridge's relay in-process, the proxy a database beside it.
#
# @use_case Development/UseCase/UC_CDC
# @facet Gherkin

Feature: The proxy hop carries token status from the domain to the proxy
  As the proxy verifying a token
  I want its status from the domain, and only its status
  So that I can refuse a revoked token without holding whose it is

  Background:
    Given a domain store and a proxy
    And the proxy hop set up on the domain store
    And the domain holding a token, its status and an identity mapping

  Scenario: Only the token's status leaves on the proxy hop
    When the proxy hop runs one cycle
    Then the server holds 1 record of the proxy hop, the token's status
    And no record of the proxy hop names the token's subject or the identity

  Scenario: The proxy holds the status, and a revoke follows it
    When the proxy hop runs one cycle and the proxy applies it
    Then the proxy holds only the token's status, not revoked
    When the domain revokes the token
    And the proxy hop runs one cycle and the proxy applies it
    Then the proxy holds only the token's status, revoked
    When the proxy applies the hop again
    Then it puts nothing new and holds 2 revisions

  Scenario: The proxy puts only token status, whatever arrives
    When the default hop ships the whole plane table
    And the proxy applies the default hop's records
    Then it puts 1, skips 2, and holds only the token's status
    And the status row has a different operationId on each hop

  Scenario: The proxy hop is not widened or narrowed from the command line
    Then setting up the proxy hop on another table is refused
    And resetting or clearing its filter is refused
    And its filter is the token-status prefix
    When the proxy hop is torn down
    Then the default hop's slot is still there
